"""基础流操作"""

# simmc/operation/Fluent/base.py
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from functools import partial
from typing import Any, Optional, Self

from ...schemas.typing import TimeDeltaLike
from ...security import json_export
from ...utils.logger import logger
from ..actions import jump_action
from ..player_control import PlayerControl

_global_control: Optional[PlayerControl] = None


def _to_sec(value: TimeDeltaLike) -> float:
    """把TimeDeltaLike类型转float类型"""
    if isinstance(value, timedelta):
        return value.total_seconds()
    return float(value)


def fluent_init_control(c: PlayerControl) -> None:
    """
    要使用流模块，必须先调用此用来初始化
    """
    global _global_control
    _global_control = c
    logger.debug("玩家控制器设置完成，后续可以使用流操作")


# -------------- 基类：所有 Fluent 命令的骨架 --------------
class FluentBase:
    """Fluent API 基类：负责生成处理器 + 统一收口 ctrl.request"""

    _timeout: TimeDeltaLike = 10.0
    _fut: asyncio.Future[None] | None = None

    def timeout(self, sec: TimeDeltaLike) -> Self:
        self._timeout = sec
        return self

    def __await__(self):
        """ 语法糖，可以不用调用终结方法，直接`await`链条"""
        return self.execute().__await__()

    def __rshift__(self, other: FluentBase | SeqChain) -> SeqChain:
        """链接下一个操作"""
        return SeqChain(self) >> other

    async def execute(self) -> None:
        """提交到玩家控制队列，唯一正确入口"""
        if not _global_control:
            raise RuntimeError("未设置PlayerControl, 请先 fluent_init_control")
        handler = self._build_handler()
        self._fut = await _global_control.request(handler)
        await asyncio.wait_for(self._fut, _to_sec(self._timeout))

    def cancel(self) -> None:
        """取消"""
        if self._fut and not self._fut.done():
            self._fut.cancel()

    def on_done(self, cb: Callable[[asyncio.Future], None]) -> Self:
        """成功回调"""
        if self._fut:
            self._fut.add_done_callback(cb)
        return self

    # 子类必须实现：返回 async (**kw) -> None
    def _build_handler(self) -> Callable[..., Awaitable[None]]:
        raise NotImplementedError


class SeqChain:
    """顺序链：把多个 Builder 按 >> 串起来，一次性排队执行"""

    __slots__ = ("_steps",)

    def __init__(self, *steps: FluentBase) -> None:
        self._steps = list(steps)

    def timeout_all(self, sec: TimeDeltaLike) -> TimeoutChain:
        """返回一个可等待包装，整体超时 sec 秒"""
        return TimeoutChain(self, sec)

    def __rshift__(self, other: FluentBase | SeqChain) -> SeqChain:
        """链接下一个操作"""
        if isinstance(other, SeqChain):
            return SeqChain(*self._steps, *other._steps)
        return SeqChain(*self._steps, other)

    def __await__(self):
        return self._run().__await__()

    async def _run(self) -> None:
        for step in self._steps:
            await step.execute()  # 每条都走 PlayerControl 队列

    def futures(self) -> list[asyncio.Future[None]]:
        return [s._fut for s in self._steps if s._fut]


class TimeoutChain:
    __slots__ = ("_chain", "_sec")

    def __init__(self, chain: SeqChain, sec: TimeDeltaLike) -> None:
        self._chain = chain
        self._sec = sec

    def __await__(self):
        return asyncio.wait_for(self._chain._run(), _to_sec(self._sec)).__await__()


class ServerCommand:
    """服务器命令"""

    def __init__(self) -> None:
        self._cmd_line: list[str] = []

    def _check_null(self, value: Any) -> bool:
        """确认是不是空指令"""
        return value == "" or value is None

    def _add_cmd(self, cmd: str) -> None:
        """添加命令"""
        self._cmd_line.append(cmd)

    def _get_cmd_line(self) -> list[str]:
        """获取命令列表"""
        return self._cmd_line


class JumpFluent(FluentBase):
    def __init__(self, times: int = 3) -> None:
        self.times = times
        self._interval = 1.0

    @json_export
    def interval(self, sec: TimeDeltaLike) -> Self:
        self._interval = sec
        return self

    def _build_handler(self) -> Callable[..., Awaitable[None]]:
        return partial(
            jump_action,
            times=self.times,
            interval=_to_sec(self._interval),
        )


class FluentBlock(FluentBase):
    """流阻塞"""

    def __init__(self, sec: TimeDeltaLike) -> None:
        self._sec = sec

    def _build_handler(self) -> Callable[..., Awaitable[None]]:
        return lambda **kw: asyncio.sleep(_to_sec(self._sec))


def jump(times: int = 3) -> JumpFluent:
    """跳跃"""
    return JumpFluent(times)


def fluent_wait(sec: TimeDeltaLike) -> FluentBlock:
    """让流等待"""
    return FluentBlock(sec)


async def fire(fluent: FluentBase) -> None:
    """立刻把 DSL 构建器塞进 PlayerControl 队列并等待完成"""
    await fluent.execute()
