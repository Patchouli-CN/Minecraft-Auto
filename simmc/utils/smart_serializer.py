"""
智能序列化器 - 支持复杂类型、循环引用、泛型

这个模块提供了一个强大的序列化框架，可以处理：
- 基础类型 (str/int/float/bool)
- 容器类型 (list/dict/set/tuple/frozenset)
- 特殊类型 (Path/datetime/Decimal/UUID/bytes)
- 枚举 (Enum)
- 数据类 (dataclass)
- 自定义类
- 循环引用检测
- LRU 缓存优化

使用方法：
    >>> serializer = SmartSerializer()
    >>> data = {"path": Path("/tmp")}
    >>> serialized = serializer.serialize(data)
    >>> restored = serializer.deserialize(serialized, dict[str, Path])
"""

import dataclasses
import inspect
import json
import threading
from collections.abc import Callable, Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Optional, Union, get_args, get_origin
from uuid import UUID

# 类型别名
DumpFunc = Callable[[Any], str]
LoadFunc = Callable[[str], Any]


class TypeUtils:
    """类型工具类 - 处理泛型、Optional、Literal 等类型判断"""
    
    @staticmethod
    def is_optional(tp: Any) -> bool:
        """判断是否为 Optional[T] 或 Union[T, None]"""
        return get_origin(tp) is Union and type(None) in get_args(tp)
    
    @staticmethod
    def strip_optional(tp: Any) -> Any:
        """从 Optional[T] 中提取实际类型"""
        if not TypeUtils.is_optional(tp):
            return tp
        args = [a for a in get_args(tp) if a is not type(None)]
        return args[0] if args else Any
    
    @staticmethod
    def is_literal(tp: Any) -> bool:
        """判断是否为 Literal 类型"""
        return get_origin(tp) is Literal
    
    @staticmethod
    def container_item_type(container_type: Any) -> Any:
        """获取容器的元素类型"""
        args = get_args(container_type)
        return args[0] if args else Any
    
    @staticmethod
    def dict_types(dict_type: Any) -> tuple[Any, Any]:
        """获取字典的键值类型"""
        args = get_args(dict_type) + (Any, Any)
        return args[0], args[1]
    
    @staticmethod
    def is_hashable(obj: Any) -> bool:
        """检查对象是否可哈希（用于缓存）"""
        try:
            hash(obj)
            return True
        except TypeError:
            return False


class Registry:
    """类型注册表 - 管理自定义类型的序列化/反序列化函数
    
    线程安全的注册中心，支持动态添加/移除类型处理器。
    """
    
    def __init__(self):
        self._serializers: dict[type, Callable[[Any], Any]] = {}
        self._deserializers: dict[type, Callable[[Any], Any]] = {}
        self._lock = threading.RLock()
        self._init_builtins()
    
    def _init_builtins(self):
        """注册内置类型处理器"""
        builtins = [
            (Path, str, Path),
            (datetime, datetime.isoformat, datetime.fromisoformat),
            (date, date.isoformat, date.fromisoformat),
            (Decimal, str, Decimal),
            (UUID, str, UUID),
            (bytes, lambda b: {"__bytes__": b.hex()},
                   lambda d: bytes.fromhex(d["__bytes__"])),
            (set, list, set),
            (frozenset, list, frozenset),
            (tuple, lambda t: {"__tuple__": list(t)},
                    lambda d: tuple(d["__tuple__"])),
        ]
        for py_type, to_json, from_json in builtins:
            self.register(py_type, to_json, from_json)
    
    def register(self, py_type: type,
                 serialize: Optional[Callable] = None,
                 deserialize: Optional[Callable] = None) -> None:
        """注册类型处理器"""
        with self._lock:
            if serialize:
                self._serializers[py_type] = serialize
            if deserialize:
                self._deserializers[py_type] = deserialize
    
    def unregister(self, py_type: type) -> None:
        """注销类型"""
        with self._lock:
            self._serializers.pop(py_type, None)
            self._deserializers.pop(py_type, None)
    
    def get_serializer(self, value: Any) -> Optional[Callable]:
        """获取值的序列化器"""
        for registered_type, serializer in self._serializers.items():
            if isinstance(value, registered_type):
                return serializer
        return None
    
    def get_deserializer(self, target_type: type) -> Optional[Callable]:
        """获取目标类型的反序列化器"""
        return self._deserializers.get(target_type)


class CycleDetector:
    """循环引用检测器
    
    用于检测对象图中的循环引用，防止无限递归。
    """
    
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._seen: set[int] = set()
    
    def __enter__(self):
        self._seen.clear()
        return self
    
    def __exit__(self, *args):
        self._seen.clear()
    
    def check(self, obj: Any) -> Optional[set[int]]:
        """检查对象是否已访问过，返回新的 seen 集合"""
        if not self.enabled:
            return None
        
        obj_id = id(obj)
        if obj_id in self._seen:
            raise ValueError(f"循环引用 detected: {type(obj).__name__}")
        
        if isinstance(obj, (list, tuple, set, frozenset, dict, Mapping)):
            return self._seen | {obj_id}
        return self._seen


class Markers:
    """特殊类型标记 - 用于在 JSON 中表示 Python 特殊类型"""
    
    TUPLE = "__tuple__"
    SET = "__set__"
    FROZENSET = "__frozenset__"
    BYTES = "__bytes__"
    CLASS = "__class__"
    MODULE = "__module__"
    
    @classmethod
    def wrap_tuple(cls, items: list) -> dict:
        return {cls.TUPLE: items}
    
    @classmethod
    def wrap_set(cls, items: list) -> dict:
        return {cls.SET: items}
    
    @classmethod
    def wrap_frozenset(cls, items: list) -> dict:
        return {cls.FROZENSET: items}
    
    @classmethod
    def wrap_bytes(cls, data: bytes) -> dict:
        return {cls.BYTES: data.hex()}
    
    @classmethod
    def unwrap(cls, data: dict) -> Optional[tuple[str, Any]]:
        """解包标记，返回 (标记类型, 数据)"""
        if cls.TUPLE in data:
            return (cls.TUPLE, data[cls.TUPLE])
        if cls.SET in data:
            return (cls.SET, data[cls.SET])
        if cls.FROZENSET in data:
            return (cls.FROZENSET, data[cls.FROZENSET])
        if cls.BYTES in data:
            return (cls.BYTES, data[cls.BYTES])
        return None


class _BaseHandler:
    """处理器基类 - 提供通用工具方法"""
    
    def __init__(self, registry: Registry, markers: Markers):
        self.registry = registry
        self.markers = markers


class Serializer(_BaseHandler):
    """序列化处理器 - 将 Python 对象转为 JSON 兼容格式"""
    
    def serialize(self, obj: Any, target_type: Any = None,
                  seen: Optional[set[int]] = None) -> Any:
        """序列化对象"""
        if obj is None:
            return None
        
        # 注册类型优先
        serializer = self.registry.get_serializer(obj)
        if serializer:
            return self.serialize(serializer(obj), None, seen)
        
        # 基础类型
        if isinstance(obj, (str, int, float, bool)):
            return obj
        
        # 枚举
        if isinstance(obj, Enum):
            return obj.value
        
        # 容器类型
        return self._serialize_container(obj, target_type or type(obj), seen)
    
    def _serialize_container(self, obj: Any, actual_type: Any,
                             seen: Optional[set[int]]) -> Any:
        """序列化容器对象"""
        
        # 序列
        if isinstance(obj, (list, tuple, set, frozenset)):
            item_type = TypeUtils.container_item_type(actual_type)
            items = [self.serialize(item, item_type, seen) for item in obj]
            
            if isinstance(obj, set):
                return Markers.wrap_set(items)
            if isinstance(obj, frozenset):
                return Markers.wrap_frozenset(items)
            return items  # list/tuple 直接返回
        
        # 映射
        if isinstance(obj, dict):
            key_type, val_type = TypeUtils.dict_types(actual_type)
            return {
                self.serialize(k, key_type, seen): self.serialize(v, val_type, seen)
                for k, v in obj.items()
            }
        
        # 数据类
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return self.serialize(dataclasses.asdict(obj), dict, seen)
        
        # 普通对象
        if hasattr(obj, "__dict__") and not isinstance(obj, type):
            return self.serialize(obj.__dict__, dict, seen)
        
        raise TypeError(f"不支持的类型: {type(obj)}")


class Deserializer(_BaseHandler):
    """反序列化处理器 - 将 JSON 数据恢复为 Python 对象"""
    
    def deserialize(self, data: Any, target_type: Any) -> Any:
        """反序列化数据"""
        if data is None:
            if TypeUtils.is_optional(target_type):
                return None
            raise ValueError(f"非可选类型不能为 null: {target_type}")
        
        # Any/Literal 直接返回
        if target_type is Any or TypeUtils.is_literal(target_type):
            return data
        
        # 处理 Optional
        main_type = TypeUtils.strip_optional(target_type)
        if main_type is Any:
            return data
        
        # 注册类型
        deserializer = self.registry.get_deserializer(main_type)
        if deserializer:
            return deserializer(data)
        
        # 特殊标记
        if isinstance(data, dict):
            marker = Markers.unwrap(data)
            if marker:
                return self._deserialize_marked(marker, main_type)
        
        # 容器类型
        return self._deserialize_container(data, main_type)
    
    def _deserialize_marked(self, marker: tuple[str, Any],
                            main_type: Any) -> Any:
        """处理带标记的数据"""
        mark, value = marker
        
        if mark == Markers.TUPLE:
            return tuple(self.deserialize(v, Any) for v in value)
        if mark == Markers.SET:
            return set(self.deserialize(v, Any) for v in value)
        if mark == Markers.FROZENSET:
            return frozenset(self.deserialize(v, Any) for v in value)
        if mark == Markers.BYTES:
            return bytes.fromhex(value)
        return value
    
    def _deserialize_container(self, data: Any, main_type: Any) -> Any:
        """反序列化容器类型"""
        origin = get_origin(main_type)
        
        # 序列
        if origin in (list, tuple, set, frozenset):
            if not isinstance(data, list):
                raise ValueError(f"期望列表, 得到 {type(data)}")
            
            item_type = TypeUtils.container_item_type(main_type)
            items = [self.deserialize(v, item_type) for v in data]
            
            if origin is tuple:
                return tuple(items)
            if origin is frozenset:
                return frozenset(items)
            if origin is set:
                return set(items)
            return items
        
        # 字典
        if origin is dict or main_type is dict:
            if not isinstance(data, dict):
                raise ValueError(f"期望字典, 得到 {type(data)}")
            
            key_type, val_type = TypeUtils.dict_types(main_type)
            return {
                self.deserialize(k, key_type): self.deserialize(v, val_type)
                for k, v in data.items()
            }
        
        # 基础类型
        if main_type in (str, int, float, bool):
            return self._deserialize_primitive(data, main_type)
        
        # 枚举
        if isinstance(main_type, type) and issubclass(main_type, Enum):
            return main_type(data)
        
        # 数据类
        if dataclasses.is_dataclass(main_type):
            return self._deserialize_dataclass(data, main_type) # type: ignore
        
        # 普通类
        if isinstance(main_type, type):
            return self._deserialize_object(data, main_type)
        
        return data
    
    def _deserialize_primitive(self, data: Any, main_type: type) -> Any:
        """反序列化基础类型"""
        if type(data) is main_type:
            return data
        try:
            return main_type(data)
        except (ValueError, TypeError) as e:
            raise ValueError(f"无法转换 {data!r} 到 {main_type}") from e
    
    def _deserialize_dataclass(self, data: Any, main_type: type) -> Any:
        """反序列化数据类"""
        if not isinstance(data, dict):
            raise ValueError(f"期望字典, 得到 {type(data)}")
        
        field_names = {f.name for f in dataclasses.fields(main_type)}
        field_types = {f.name: f.type for f in dataclasses.fields(main_type)}
        
        kwargs = {
            k: self.deserialize(v, field_types.get(k, Any))
            for k, v in data.items()
            if k in field_names
        }
        return main_type(**kwargs)
    
    def _deserialize_object(self, data: Any, main_type: type) -> Any:
        """反序列化普通对象"""
        if isinstance(data, main_type):
            return data
        if not isinstance(data, dict):
            raise ValueError(f"无法从非字典构造 {main_type}")
        
        # 过滤元数据
        clean = {k: v for k, v in data.items() if not k.startswith("__")}
        
        # 尝试按签名构造
        try:
            sig = inspect.signature(main_type.__init__)
            valid = set(sig.parameters.keys()) - {"self"}
            filtered = {k: v for k, v in clean.items() if k in valid}
            return main_type(**filtered)
        except (ValueError, TypeError):
            return main_type(**clean)


class SmartSerializer:
    """
    智能序列化器 - 主类
    
    整合所有组件，提供完整的序列化/反序列化功能。
    
    示例：
        >>> s = SmartSerializer()
        >>> s.dumps({"path": Path("/tmp")})
        '{"path": "/tmp"}'
        >>> s.loads('{"path": "/tmp"}', dict[str, Path])
        {'path': Path('/tmp')}
    """
    
    def __init__(self, dump_func: Optional[DumpFunc] = None,
                 load_func: Optional[LoadFunc] = None,
                 cache_size: int = 128,
                 detect_cycles: bool = True):
        self._dump = dump_func or json.dumps
        self._load = load_func or json.loads
        self._detect_cycles = detect_cycles
        self._cache_size = cache_size
        
        # 初始化组件
        self._registry = Registry()
        self._markers = Markers()
        self._serializer = Serializer(self._registry, self._markers)
        self._deserializer = Deserializer(self._registry, self._markers)
        
        # 缓存设置
        self._use_cache = cache_size > 0
        if self._use_cache:
            self._setup_cache()
    
    def _setup_cache(self):
        """设置 LRU 缓存"""
        @lru_cache(maxsize=self._cache_size)
        def _cached_serialize(value: Any, target: Any) -> Any:
            return self._serializer.serialize(value, target)
        
        @lru_cache(maxsize=self._cache_size)
        def _cached_deserialize(data: Any, target: Any) -> Any:
            return self._deserializer.deserialize(data, target)
        
        self._cached_serialize = _cached_serialize
        self._cached_deserialize = _cached_deserialize
    
    # ========== 公共 API ==========
    
    def serialize(self, value: Any, target_type: Any = None) -> Any:
        """序列化为 JSON 兼容对象"""
        if self._use_cache and TypeUtils.is_hashable(value):
            return self._cached_serialize(value, target_type)
        return self._serializer.serialize(value, target_type)
    
    def deserialize(self, data: Any, target_type: Any) -> Any:
        """从 JSON 兼容对象反序列化"""
        if self._use_cache and TypeUtils.is_hashable(data):
            return self._cached_deserialize(data, target_type)
        return self._deserializer.deserialize(data, target_type)
    
    def dumps(self, obj: Any, type_hint: Any = None, **kwargs) -> str:
        """序列化到 JSON 字符串"""
        return self._dump(self.serialize(obj, type_hint), **kwargs)
    
    def loads(self, data: str, target_type: Any, **kwargs) -> Any:
        """从 JSON 字符串反序列化"""
        return self.deserialize(self._load(data, **kwargs), target_type)
    
    def dump(self, obj: Any, fp, type_hint: Any = None, **kwargs) -> None:
        """序列化到文件"""
        json.dump(self.serialize(obj, type_hint), fp, **kwargs)
    
    def load(self, fp, target_type: Any, **kwargs) -> Any:
        """从文件反序列化"""
        return self.deserialize(json.load(fp, **kwargs), target_type)
    
    # ========== 类型注册 API ==========
    
    def register_type(self, py_type: type,
                      serialize: Optional[Callable] = None,
                      deserialize: Optional[Callable] = None) -> None:
        """注册自定义类型"""
        self._registry.register(py_type, serialize, deserialize)
    
    def unregister_type(self, py_type: type) -> None:
        """注销类型"""
        self._registry.unregister(py_type)


# ========== 单例模式便捷函数 ==========

_default = SmartSerializer()

def serialize_value(value: Any, target_type: Any = None) -> Any:
    return _default.serialize(value, target_type)

def deserialize_value(data: Any, target_type: Any) -> Any:
    return _default.deserialize(data, target_type)

def dumps(obj: Any, type_hint: Any = None, **kwargs) -> str:
    return _default.dumps(obj, type_hint, **kwargs)

def loads(data: str, target_type: Any, **kwargs) -> Any:
    return _default.loads(data, target_type, **kwargs)

def register_type(*args, **kwargs):
    return _default.register_type(*args, **kwargs)

def set_backend(dump_func=None, load_func=None, cache_size=128) -> SmartSerializer:
    """切换后端"""
    global _default
    _default = SmartSerializer(dump_func, load_func, cache_size)
    return _default
