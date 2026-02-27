
import inspect
import re
from collections.abc import Callable
from typing import Any

from ..constants import TRIGGERS
from ..operation.fluent.base import fire, jump
from ..operation.fluent.command import chat, pay
from ..operation.fluent.land import land
from ..schemas.event import EventBase
from ..schemas.event_registry import get_event_name
from ..security import _safe_attr
from ..utils.logger import logger

_CMD_MAP: dict[str, Callable[..., Any]] = {
    "chat": chat,
    "jump": jump,
    "pay":  pay,
    "land": land
}

class JsonTriggerService:
    """纯配置化触发器服务，挂载即生效"""

    async def handle(self, ev: EventBase) -> None:
        name = get_event_name(ev)
        if not name:
            return

        for rule in TRIGGERS:
            if rule["on"] != name or not self._match_when(ev, rule["when"]):
                continue

            cmd = rule["do"]["cmd"]
            args = rule["do"].get("args", {})
            ctor = _CMD_MAP.get(cmd)
            if not ctor:
                logger.warning(f"未知指令: {cmd}")
                continue

            # ⭐ 关键：自动补缺失字段
            args = self._fill_missing_args(ctor, args, ev)
            logger.trace(f"事件<{name}> 缺少字段自动填入: {cmd}({args})")
            fluent = ctor(**args)
            for meth, *argv in rule["do"]["chain"]:
                attr = _safe_attr(fluent, meth)      # 安检
                if callable(attr):
                    fluent = attr(*argv)                  # 方法调用
                else:                                     # 属性继续链
                    fluent = attr

            logger.info(f"事件<{name}> 命中 -> {cmd}({args})")
            await fire(fluent)

    # ---------- 工具 ----------
    def _match_when(self, ev: EventBase, cond: dict[str, Any]) -> bool:
        """支持字段=值 或 字段=[值列表]；可扩展正则、范围"""
        ev_dict = ev.__dict__
        for k, v in cond.items():
            ev_val = ev_dict.get(k)
            if isinstance(v, list):
                if ev_val not in v:
                    return False
            elif isinstance(v, str) and v.startswith("re:"):
                if not re.search(v[3:], str(ev_val)):
                    return False
            else:
                if ev_val != v:
                    return False
        return True

    def _fill_missing_args(self, ctor: Callable, args: dict, ev: EventBase) -> dict:
        """
        用事件字段补全 args 里缺位的构造器参数。
        仅补“关键字同名且当前为 None / 缺失”的字段。
        """
        sig = inspect.signature(ctor)
        ev_dict = ev.__dict__
        filled = args.copy()
        for name, param in sig.parameters.items():
            # 构造器要求、用户没给、事件里刚好有，就自动补
            if name not in filled and name in ev_dict:
                filled[name] = ev_dict[name]
        return filled
