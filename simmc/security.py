# simmc/security/export.py
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

_ALLOW_METHOD: dict[str, set[str]] = {}

P = ParamSpec("P")
T = TypeVar("T")


def json_export(func: Callable[P, T]) -> Callable[P, T]:
    """装饰公开可链式调用的方法/属性 getter"""
    cls_name = func.__qualname__.split(".")[0]
    _ALLOW_METHOD.setdefault(cls_name, set()).add(func.__name__)
    return func


def _safe_attr(cls: type, name: str) -> Any:
    cls_name = type(cls).__name__  # 也是 str
    allowed = _ALLOW_METHOD.get(cls_name, set())
    if name not in allowed:
        raise PermissionError(
            f"{cls_name}.{name} 未在出口白名单，为了避免安全问题，拒绝执行"
        )
    return getattr(cls, name)
