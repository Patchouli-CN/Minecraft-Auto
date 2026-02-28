"""事件注册表"""

# simmc/schemas/event_registry.py
from functools import wraps
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from .event import EventBase

__key_to_cls: dict[str, type["EventBase"]] = {}
__cls_to_key: dict[type["EventBase"], str] = {}

EVENT = TypeVar("EVENT", bound="EventBase", covariant=True)


def event(name: str):
    """
    类装饰器：把 Event 子类注册到全局映射
    例：
        @event("加入")
        class JoinEvent(EventBase[str]): ...
    """

    def _decorator(cls: type[EVENT]) -> type[EVENT]:
        if name in __key_to_cls:
            raise RuntimeError(f"事件名 {name} 已被 {__key_to_cls[name]} 注册")

        # 用 wraps 保留原类元数据
        @wraps(cls)
        def _wrapper(*args, **kwargs):
            return cls(*args, **kwargs)

        # 把元数据同步到原类，并注册
        _wrapper.__dict__.update(cls.__dict__)
        __key_to_cls[name] = cls
        __cls_to_key[cls] = name
        return cls  # 返回原类，保证继承、类型检查正常

    return _decorator


def get_event(key: str) -> type["EventBase"] | None:
    """message_listener 用"""
    return __key_to_cls.get(key)


def get_event_name(ev: "EventBase") -> str | None:
    return __cls_to_key.get(type(ev))
