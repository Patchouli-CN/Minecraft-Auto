"""服务器指令"""

from collections.abc import Awaitable, Callable
from functools import partial
from typing import Optional, Self

from ...constants import CMD_CHANNEL_TABLE
from ...schemas.typing import TimeDeltaLike
from ...security import json_export
from ...utils.logger import logger
from ..type_chat import type_in_chat
from .base import FluentBase, ServerCommand, _to_sec


class ChatCommandFluent(ServerCommand, FluentBase):
    def __init__(self, content: str) -> None:
        if not content or content.strip() == "":
            raise ValueError(
                "What will I say? I don't know, "
                "maybe you will give some content for me to say it. (´・ω・`)"
            )
        self.content = content
        self._player: Optional[str] = None  # None = 公屏
        self._interval: TimeDeltaLike = 0.0
        self._channel: str = "global"

    @json_export
    def ensure_channel(self, name: str = "global") -> Self:
        """手动确认当前频道"""
        self._channel = name
        return self

    @json_export
    def switch_channel(self, channel: str) -> Self:
        # 仅给公屏消息加标签前缀，/m 不受影响
        if self._player is not None:
            logger.debug("sendto 模式，忽略频道切换")
            return self
        prefix = CMD_CHANNEL_TABLE.get(channel, self._channel)
        self._cmd_line.insert(0, f"/{prefix}")
        return self

    @json_export
    def sendto(self, player: str) -> Self:
        if not player or player.strip() == "":
            raise ValueError(
                "say to who? I say to air? ╰（‵□′）╯, Maybe you want `.to_all()` instead of `.sendto('')` ?"
            )
        self._player = player.strip()
        self._cmd_line = [f"/m {player} {self.content}"]
        return self

    @json_export
    def to_all(self) -> Self:
        self._player = None
        self._channel = CMD_CHANNEL_TABLE["全局"]
        return self

    @json_export
    def interval(self, time: TimeDeltaLike = 1.0) -> Self:
        self._interval = time
        return self

    def _build_handler(self) -> Callable[..., Awaitable[None]]:
        if self._player is None:
            text = self.content
        else:
            text = self._get_cmd_line()
        return partial(type_in_chat, text, _to_sec(self._interval))


class PayCommandFluent(ServerCommand, FluentBase):
    """/pay <player> <amount> 金币转账流"""

    def __init__(self, amount: int) -> None:
        if amount <= 0:
            raise ValueError("amount must be positive.")
        self._amount = amount
        self._target: str | None = None

    @json_export
    def transfer_to(self, player: str) -> Self:
        if not player or player.strip() == "":
            raise ValueError("player name empty.")
        self._target = player.strip()
        return self

    # -------- 内部：生成真正执行的协程 --------
    def _build_handler(self) -> Callable[..., Awaitable[None]]:
        cmd = f"/pay {self._target} {self._amount}"
        return partial(type_in_chat, cmd, 0.0)  # 0 间隔，立刻回车


def pay(amount: int) -> PayCommandFluent:
    """向玩家转账"""
    return PayCommandFluent(amount)


def chat(content: str) -> ChatCommandFluent:
    """聊天"""
    return ChatCommandFluent(content)
