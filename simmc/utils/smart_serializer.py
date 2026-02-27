import json
import threading
import dataclasses
import inspect
from functools import lru_cache
from typing import Any, get_origin, get_args, Union, Optional, Callable, Literal
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID
from collections.abc import Mapping, Set, Sequence
from enum import Enum

# 类型别名
DumpFunc = Callable[[Any], str]
LoadFunc = Callable[[str], Any]


class SmartSerializer:
    """
    增强型序列化处理器（修复版）
    """
    
    # 类级别的注册表（所有实例共享）
    _SERIALIZERS: dict[type, Callable[[Any], Any]] = {}
    _DESERIALIZERS: dict[type, Callable[[Any], Any]] = {}
    _lock = threading.RLock()
    _initialized = False
    
    # 内置类型注册
    _BUILTINS = [
        (Path, str, Path),
        (datetime, datetime.isoformat, datetime.fromisoformat),
        (date, date.isoformat, date.fromisoformat),
        (Decimal, str, Decimal),
        (UUID, str, UUID),
        (bytes, lambda b: {"__bytes__": b.hex()}, lambda d: bytes.fromhex(d["__bytes__"]) if isinstance(d, dict) and "__bytes__" in d else bytes.fromhex(d)), # type: ignore
        (set, list, set),
        (frozenset, list, frozenset),
        (tuple, lambda t: {"__tuple__": list(t)}, lambda d: tuple(d["__tuple__"]) if isinstance(d, dict) and "__tuple__" in d else tuple(d)),
    ]
    
    def __init__(
        self,
        dump_func: Optional[DumpFunc] = None,
        load_func: Optional[LoadFunc] = None,
        cache_size: int = 128,
        detect_cycles: bool = True
    ):
        self._dump = dump_func or json.dumps
        self._load = load_func or json.loads
        self._detect_cycles = detect_cycles
        self._cache_size = cache_size
        self._use_cache = cache_size > 0
        self._init_builtins()
    
    def _init_builtins(self):
        """初始化内置类型注册（线程安全，只执行一次）"""
        with self._lock:
            if not SmartSerializer._initialized:
                for py_type, to_json, from_json in self._BUILTINS:
                    self.register_type(py_type, to_json, from_json)
                SmartSerializer._initialized = True
    
    @classmethod
    def register_type(
        cls,
        py_type: type,
        serialize_func: Optional[Callable[[Any], Any]] = None,
        deserialize_func: Optional[Callable[[Any], Any]] = None
    ) -> None:
        """注册自定义类型的序列化/反序列化函数"""
        with cls._lock:
            if serialize_func:
                cls._SERIALIZERS[py_type] = serialize_func
            if deserialize_func:
                cls._DESERIALIZERS[py_type] = deserialize_func
    
    @classmethod
    def unregister_type(cls, py_type: type) -> None:
        """注销类型注册"""
        with cls._lock:
            cls._SERIALIZERS.pop(py_type, None)
            cls._DESERIALIZERS.pop(py_type, None)
    
    @staticmethod
    def _is_optional_type(tp: Any) -> bool:
        """判断是否为 Optional[T] 或 Union[T, None]"""
        origin = get_origin(tp)
        if origin is Union:
            args = get_args(tp)
            return type(None) in args
        return False
    
    @staticmethod
    def _get_non_optional_type(tp: Any) -> Any:
        """从 Optional[T] 或 Union[T, None] 提取非 None 类型"""
        if SmartSerializer._is_optional_type(tp):
            args = [a for a in get_args(tp) if a is not type(None)]
            return args[0] if args else Any
        return tp
    
    @staticmethod
    def _is_literal_type(tp: Any) -> bool:
        """判断是否为 Literal 类型"""
        return get_origin(tp) is Literal
    
    def _serialize_impl(self, value: Any, target_type: Any = None, _seen: Optional[set[int]] = None) -> Any:
        """实际的序列化实现"""
        if value is None:
            return None
        
        # 循环引用检测
        if self._detect_cycles:
            obj_id = id(value)
            if _seen is None:
                _seen = set()
            elif obj_id in _seen:
                raise ValueError(f"Circular reference detected in object {type(value).__name__} at id {obj_id}")
            
            if isinstance(value, (list, tuple, set, frozenset, dict, Mapping)):
                _seen = _seen | {obj_id}
        
        try:
            # 1. 优先检查注册类型
            for registered_type, serializer in SmartSerializer._SERIALIZERS.items():
                if isinstance(value, registered_type):
                    result = serializer(value)
                    return self._serialize_impl(result, None, _seen)
            
            # 2. 基础类型直接返回
            if isinstance(value, (str, int, float, bool)):
                return value
            
            # 3. Enum 支持
            if isinstance(value, Enum):
                return value.value
            
            # 4. 处理泛型容器（排除 str/bytes）
            actual_type = target_type or type(value)
            origin = get_origin(actual_type)
            
            if isinstance(value, (str, bytes)):
                return value
            
            if origin in (list, tuple, set, frozenset) or (origin is None and isinstance(value, (Set, Sequence))):
                item_type = get_args(actual_type)[0] if get_args(actual_type) else Any
                result = [self._serialize_impl(item, item_type, _seen) for item in value]
                if origin is set:
                    return {"__set__": result}
                elif origin is frozenset:
                    return {"__frozenset__": result}
                return result
            
            elif origin is dict or (origin is None and isinstance(value, Mapping)):
                key_type, val_type = (get_args(actual_type) + (Any, Any))[:2]
                return {
                    self._serialize_impl(k, key_type, _seen): self._serialize_impl(v, val_type, _seen)
                    for k, v in value.items() # type: ignore
                }
            
            # 5. dataclass 支持
            if dataclasses.is_dataclass(value) and not isinstance(value, type):
                return self._serialize_impl(dataclasses.asdict(value), dict, _seen)
            
            # 6. 普通自定义类：转为 dict
            if hasattr(value, '__dict__') and not isinstance(value, type):
                result = {
                    "__class__": value.__class__.__name__,
                    "__module__": value.__class__.__module__,
                    **value.__dict__
                }
                return self._serialize_impl(result, dict, _seen)
            
            raise TypeError(f"Cannot serialize {value!r} of type {type(value)} (target: {actual_type})")
        
        except Exception as e:
            if "Circular reference" in str(e):
                raise
            raise TypeError(f"Serialization failed for {type(value)}: {e}") from e
    
    def _deserialize_impl(self, ser_value: Any, target_type: Any) -> Any:
        """实际的反序列化实现（修复版）"""
        if ser_value is None:
            if self._is_optional_type(target_type):
                return None
            raise ValueError(f"Non-optional field requires non-null value, got null for type {target_type}")
        
        # 处理 Any 类型 - 关键修复！
        if target_type is Any:
            return ser_value
        
        # 处理 Literal 类型
        if self._is_literal_type(target_type):
            return ser_value
        
        # 处理 Optional[T]
        main_type = self._get_non_optional_type(target_type)
        
        # 再次检查 Any（从 Optional 中提取后可能是 Any）
        if main_type is Any:
            return ser_value
        
        # 注册类型优先
        if main_type in SmartSerializer._DESERIALIZERS:
            return SmartSerializer._DESERIALIZERS[main_type](ser_value)
        
        # 处理特殊标记
        if isinstance(ser_value, dict):
            if "__tuple__" in ser_value:
                items = [self._deserialize_impl(item, Any) for item in ser_value["__tuple__"]]
                return tuple(items)
            elif "__set__" in ser_value:
                items = [self._deserialize_impl(item, Any) for item in ser_value["__set__"]]
                return set(items)
            elif "__frozenset__" in ser_value:
                items = [self._deserialize_impl(item, Any) for item in ser_value["__frozenset__"]]
                return frozenset(items)
            elif "__bytes__" in ser_value:
                return bytes.fromhex(ser_value["__bytes__"])
        
        # 泛型容器
        origin = get_origin(main_type)
        if origin in (list, tuple, set, frozenset):
            item_type = get_args(main_type)[0] if get_args(main_type) else Any
            if not isinstance(ser_value, list):
                raise ValueError(f"Expected list for {main_type}, got {type(ser_value)}")
            items = [self._deserialize_impl(item, item_type) for item in ser_value]
            if origin is tuple:
                return tuple(items)
            elif origin is frozenset:
                return frozenset(items)
            elif origin is set:
                return set(items)
            return items
        
        elif origin is dict or main_type is dict:
            key_type, val_type = (get_args(main_type) + (Any, Any))[:2]
            if not isinstance(ser_value, dict):
                raise ValueError(f"Expected dict, got {type(ser_value)} for {main_type}")
            return {
                self._deserialize_impl(k, key_type): self._deserialize_impl(v, val_type)
                for k, v in ser_value.items()
            }
        
        # 基础类型
        if main_type in (str, int, float, bool):
            if type(ser_value) is main_type:
                return ser_value
            try:
                return main_type(ser_value) # type: ignore
            except (ValueError, TypeError) as e:
                raise ValueError(f"Cannot convert {ser_value!r} to {main_type}: {e}") from e
        
        # Enum 支持
        if isinstance(main_type, type) and issubclass(main_type, Enum):
            return main_type(ser_value)
        
        # dataclass 支持
        if dataclasses.is_dataclass(main_type):
            if not isinstance(ser_value, dict):
                raise ValueError(f"Expected dict for dataclass {main_type}, got {type(ser_value)}")
            field_names = {f.name for f in dataclasses.fields(main_type)}
            filtered = {k: v for k, v in ser_value.items() if k in field_names}
            field_types = {f.name: f.type for f in dataclasses.fields(main_type)}
            kwargs = {
                k: self._deserialize_impl(v, field_types.get(k, Any))
                for k, v in filtered.items()
            }
            return main_type(**kwargs) # type: ignore
        
        # 自定义类：尝试用 dict 初始化 - 关键修复！
        if isinstance(main_type, type) and not isinstance(ser_value, main_type):
            if isinstance(ser_value, dict):
                # 过滤元数据字段，并只保留 __init__ 接受的参数
                clean_dict = {k: v for k, v in ser_value.items() if not k.startswith('__')}
                
                # 检查 __init__ 签名，只传入接受的参数
                try:
                    sig = inspect.signature(main_type.__init__)
                    valid_params = set(sig.parameters.keys()) - {'self'}
                    filtered_dict = {k: v for k, v in clean_dict.items() if k in valid_params}
                    
                    return main_type(**filtered_dict)
                except (ValueError, TypeError):
                    # 如果检查签名失败，尝试直接传入
                    return main_type(**clean_dict)
            
            raise ValueError(f"Cannot construct {main_type} from non-dict value: {ser_value}")
        
        # 已是目标类型
        return ser_value
    
    def serialize(self, value: Any, target_type: Any = None) -> Any:
        """序列化为兼容对象"""
        if self._use_cache and self._is_hashable(value):
            return self._cached_serialize(value, target_type)
        return self._serialize_impl(value, target_type, None)
    
    def deserialize(self, ser_value: Any, target_type: Any) -> Any:
        """反序列化为 Python 对象"""
        if self._use_cache and self._is_hashable(ser_value):
            return self._cached_deserialize(ser_value, target_type)
        return self._deserialize_impl(ser_value, target_type)
    
    @staticmethod
    def _is_hashable(obj: Any) -> bool:
        """检查对象是否可哈希"""
        try:
            hash(obj)
            return True
        except TypeError:
            return False
    
    @lru_cache(maxsize=128)
    def _cached_serialize(self, value: Any, target_type: Any) -> Any:
        """带缓存的序列化（仅用于不可变对象）"""
        return self._serialize_impl(value, target_type, None)
    
    @lru_cache(maxsize=128)
    def _cached_deserialize(self, ser_value: Any, target_type: Any) -> Any:
        """带缓存的反序列化（仅用于不可变对象）"""
        return self._deserialize_impl(ser_value, target_type)
    
    def dumps(self, obj: Any, type_hint: Any = None, **kwargs) -> str:
        """序列化为字符串"""
        serialized = self.serialize(obj, type_hint)
        return self._dump(serialized, **kwargs)
    
    def loads(self, data_str: str, target_type: Any, **kwargs) -> Any:
        """从字符串反序列化"""
        data = self._load(data_str, **kwargs)
        return self.deserialize(data, target_type)
    
    def dump(self, obj: Any, fp, type_hint: Any = None, **kwargs) -> None:
        """序列化到文件对象"""
        serialized = self.serialize(obj, type_hint)
        json.dump(serialized, fp, **kwargs)
    
    def load(self, fp, target_type: Any, **kwargs) -> Any:
        """从文件对象反序列化"""
        data = json.load(fp, **kwargs)
        return self.deserialize(data, target_type)


# 默认实例（向后兼容）
_default_instance = SmartSerializer()

# 模块级便捷函数（向后兼容）
def set_backend(dump_func: Optional[DumpFunc] = None, load_func: Optional[LoadFunc] = None, cache_size: int = 128):
    """切换后端并返回新的 SmartSerializer 实例"""
    global _default_instance
    _default_instance = SmartSerializer(dump_func=dump_func, load_func=load_func, cache_size=cache_size)
    return _default_instance

def register_type(*args, **kwargs):
    return SmartSerializer.register_type(*args, **kwargs)

def serialize_value(value: Any, target_type: Any = None) -> Any:
    return _default_instance.serialize(value, target_type)

def deserialize_value(data_value: Any, target_type: Any) -> Any:
    return _default_instance.deserialize(data_value, target_type)

def dumps(obj: Any, type_hint: Any = None, **kwargs) -> str:
    return _default_instance.dumps(obj, type_hint, **kwargs)

def loads(data_str: str, target_type: Any, **kwargs) -> Any:
    return _default_instance.loads(data_str, target_type, **kwargs)