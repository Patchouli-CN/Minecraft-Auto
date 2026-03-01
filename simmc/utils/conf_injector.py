import inspect
import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Optional, TypeVar

from .smart_serializer import deserialize_value, serialize_value

_LOCK = threading.Lock()

_T = TypeVar("_T", bound=type)

_INJECTOR_CONFIG_PATH = None

try:
    if not _INJECTOR_CONFIG_PATH:
        from ..constants import CONFIG_FILE

        _INJECTOR_CONFIG_PATH = CONFIG_FILE
except ImportError:
    _INJECTOR_CONFIG_PATH = Path("./config.json")


def set_config_path(path: str | Path | None = None) -> None:
    """设置injector全局配置路径"""
    global _INJECTOR_CONFIG_PATH
    _INJECTOR_CONFIG_PATH = Path(path or "")


class ConfigSession:
    def __init__(
        self,
        path: Path,
        instance: type,
        field_types: dict[str, type],
        class_key: str,
        fields_to_inject: Optional[set[str]] = None,
        readonly: bool = False,
    ):
        self._readonly = readonly
        self._path = path
        self._instance = instance
        self._field_types = field_types
        self._class_key = class_key
        self._fields_to_inject = fields_to_inject or set(field_types.keys())

    def __enter__(self):
        full_config = {}
        class_config = {}

        if _INJECTOR_CONFIG_PATH.exists():  # type: ignore
            with open(self._path, "r", encoding="utf-8") as f:
                try:
                    full_config = json.load(f)
                    class_config = full_config.get(self._class_key, {})
                except (json.JSONDecodeError, OSError):
                    pass  # 文件损坏或无法读，用空配置

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
                print(
                    f"[Config] Warning: Failed to load '{self._class_key}.{key}': {e}. Using default."
                )

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 1. 读取完整配置
        full_config = {}
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    full_config = json.load(f)
            except (json.JSONDecodeError, OSError):
                pass

        # 2. 构建当前类的配置块
        class_block = {}
        for key, field_type in self._field_types.items():
            try:
                val = getattr(self._instance, key)
                class_block[key] = serialize_value(val, field_type)
            except Exception as e:
                print(f"[Config] Skip saving '{self._class_key}.{key}': {e}")

        # 3. 更新完整配置
        full_config[self._class_key] = class_block

        # 4. 原子写入（如果只读则不写入）
        if not self._readonly:
            with _LOCK:
                fd, tmp_path = tempfile.mkstemp(dir=self._path.parent, suffix=".tmp")
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        json.dump(full_config, f, indent=2, ensure_ascii=False)
                    os.replace(tmp_path, self._path)
                except Exception:
                    os.unlink(tmp_path)
                    raise


class ConfigInject:
    def __init__(
        self,
        name: str | None = None,
        at: Optional[set[str]] = None,
        config_file: Optional[Path] = None,
    ) -> None:
        self.conf_path = config_file or _INJECTOR_CONFIG_PATH
        self._fields = set(at) if at else None
        self._conf_name = name

    def __call__(self, cls: _T) -> _T:
        original_init = cls.__init__
        conf_name = (
            self._conf_name or cls.__qualname__
        )  # 使用类名或自定义名，如 "MyClass" 或 "Outer.Inner"

        def new_init(instance, *args, **kwargs):
            original_init(instance, *args, **kwargs)

            annotations = inspect.get_annotations(cls)
            field_types = {}
            for name, typ in annotations.items():
                if hasattr(cls, name):  # 必须有默认值（类属性）
                    field_types[name] = typ
                else:
                    raise ValueError(
                        f"Field '{name}' has no default value. Required for @Inject."
                    )

            fields_to_inject = self._fields or set(field_types.keys())

            with ConfigSession(
                self.conf_path, instance, field_types, conf_name, fields_to_inject # type: ignore
            ):
                pass

        cls.__init__ = new_init
        return cls
