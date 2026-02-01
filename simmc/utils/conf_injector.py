import json
import threading
import os
import tempfile
from pathlib import Path
from typing import Optional, TypeVar, get_type_hints, Any

from .smartjson import serialize_value, deserialize_value
from ..constants import _CONF_FILE

WARPED_CLS = TypeVar("WARPED_CLS", bound=type)
_LOCK = threading.Lock()


def _get_configurable_fields(
    cls: type,
    explicit_fields: Optional[set[str]] = None,
    instance_defaults: dict[str, Any] | None = None
) -> tuple[dict[str, type], set[str]]:
    """
    安全获取可用于配置注入/保存的字段。
    
    :param cls: 被注入的类
    :param explicit_fields: 如果提供，则只考虑这些字段（白名单）
    :param instance_defaults: 类属性默认值字典（用于验证存在性）
    :return: (field_types, valid_field_names)
    """
    instance_defaults = instance_defaults or {}
    annotations = get_type_hints(cls)
    field_types = {}
    valid_fields = set()

    candidates = explicit_fields if explicit_fields is not None else annotations.keys()

    for name in candidates:
        # 字段必须在类型注解中
        if name not in annotations:
            continue
        # 字段必须有类属性默认值（即存在于类中）
        if name not in instance_defaults:
            continue
        # 排除私有属性（以单下划线或双下划线开头，但允许 __xxx__ 魔法方法？不，配置不用魔法方法）
        if name.startswith('_'):
            continue
        # 排除可调用对象（方法等）
        attr = getattr(cls, name, None)
        if callable(attr):
            continue

        field_types[name] = annotations[name]
        valid_fields.add(name)

    return field_types, valid_fields


class ConfigSession:
    def __init__(
        self,
        path: Path,
        instance: object,
        field_types: dict[str, type],
        class_key: str,
        fields_to_inject: set[str],
        readonly: bool = False
    ):
        self._readonly = readonly
        self._path = path
        self._instance = instance
        self._field_types = field_types
        self._class_key = class_key
        self._fields_to_inject = fields_to_inject  # 现在保证是安全子集

    def __enter__(self):
        full_config = {}
        class_config = {}
        if self._path.exists():
            with open(self._path, 'r', encoding='utf-8') as f:
                try:
                    full_config = json.load(f)
                    class_config = full_config.get(self._class_key, {})
                except (json.JSONDecodeError, OSError):
                    pass

        for key in self._fields_to_inject:
            if not hasattr(self._instance, key):
                continue
            default_val = getattr(self._instance, key)
            field_type = self._field_types[key]
            raw_val = class_config.get(key, default_val)
            try:
                injected_val = deserialize_value(raw_val, field_type)
                setattr(self._instance, key, injected_val)
            except Exception as e:
                print(f"[Config] Warning: Failed to load '{self._class_key}.{key}': {e}. Using default.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        full_config = {}
        if self._path.exists():
            try:
                with open(self._path, 'r', encoding='utf-8') as f:
                    full_config = json.load(f)
            except (json.JSONDecodeError, OSError):
                pass

        class_block = {}
        for key in self._fields_to_inject:  # 只保存要注入的字段
            field_type = self._field_types[key]
            try:
                val = getattr(self._instance, key)
                class_block[key] = serialize_value(val, field_type)
            except Exception as e:
                print(f"[Config] Skip saving '{self._class_key}.{key}': {e}")

        full_config[self._class_key] = class_block

        if not self._readonly:
            with _LOCK:
                fd, tmp_path = tempfile.mkstemp(dir=self._path.parent, suffix='.tmp')
                try:
                    with os.fdopen(fd, 'w', encoding='utf-8') as f:
                        json.dump(full_config, f, indent=2, ensure_ascii=False)
                    os.replace(tmp_path, self._path)
                except Exception:
                    os.unlink(tmp_path)
                    raise


class Inject:
    def __init__(self, at: Optional[set[str]] = None, config_file: Optional[Path] = None, readonly: bool = False):
        self.conf_path = config_file or _CONF_FILE
        self._fields = set(at) if at else None
        self.readonly = readonly

    def __protect_class(self, need_protect: object) -> None:
        def protect(name: str, value) -> None:
            raise AttributeError(f"property {name} cannot set, because this class is read-only.")
        need_protect.__setattr__ = protect

    def __call__(self, cls: WARPED_CLS) -> WARPED_CLS:
        original_init = cls.__init__
        class_key = cls.__qualname__

        if self.readonly:
            self.__protect_class(cls)

        def new_init(instance, *args, **kwargs):
            original_init(instance, *args, **kwargs)

            # 收集类属性默认值（用于验证字段存在性）
            instance_defaults = {}
            for name in dir(cls):
                if name.startswith('_'):
                    continue
                attr = getattr(cls, name)
                if not callable(attr):  # 排除方法
                    instance_defaults[name] = attr

            # 👇 关键修改：使用安全字段发现
            field_types, valid_fields = _get_configurable_fields(
                cls,
                explicit_fields=self._fields,
                instance_defaults=instance_defaults
            )

            if not field_types:
                raise ValueError(f"No valid configurable fields found in {cls.__name__}. "
                                 f"Ensure fields have type annotations and default values, and are not private.")

            fields_to_inject = valid_fields  # 已经是安全子集

            with ConfigSession(
                self.conf_path,
                instance,
                field_types,
                class_key,
                fields_to_inject,
                readonly=self.readonly
            ):
                pass

        cls.__init__ = new_init
        return cls