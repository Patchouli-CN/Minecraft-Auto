""" 聊天频道确认模块 """

import asyncio
import re
import uuid

from ..operation.fluent.base import fire
from ..operation.fluent.command import chat
from ..schemas.event import MessageEvent
from ..utils.logger import logger

# 前缀 → 频道名
_PREFIX_MAP: dict[str, str] = {
    r"^\[G\]": "global",
    r"^\[L\]": "local",
    r"^国家\s+\w+\s*\|": "nations chat",
    r"^\[交易\]": "tc",
    r"^\[RP\]": "rp"
}

# 预编译正则
_COMPILED = {re.compile(p): v for p, v in _PREFIX_MAP.items()}

class ChannelEnsureService:
    """只看『自己发的聊天行』来推断当前频道，带探针防止被洪水覆盖"""

    def __init__(self, my_id: str, timeout: float = 1.0) -> None:
        self._my_id = my_id
        self._timeout = timeout
        self._current: str = "global"
        self._probe_prefix = "channel_scan:"
        self._pending_probe: dict[str, asyncio.Future[str]] = {} # 探针缓存
        self._lock = asyncio.Lock()

    # -------- 事件总线唯一入口 --------
    async def handle(self, ev: MessageEvent) -> None:
        content = ev.content
        # 1. 必须自己发的行
        if self._my_id not in content:
            return

        # 2. 如果是正在等待的探针，唤醒对应 Future
        for uid, fut in list(self._pending_probe.items()):
            if f"{self._probe_prefix}{uid}" in content and not fut.done():   # 用“.uid”当标记
                channel = self._parse_channel(content)
                logger.debug(f"抓到探针, 探针返回结果: {channel}")
                fut.set_result(channel)
                # 顺便更新缓存
                async with self._lock:
                    self._current = channel
                return

        # 3. 普通闲聊也解析，保持缓存最新
        channel = self._parse_channel(content)
        async with self._lock:
            self._current = channel

    # -------- 业务代码查询接口 --------
    async def get_channel(self) -> str:
        """发探针 -> 等探针回来 -> 返回那一刻频道"""
        uid = uuid.uuid4().hex[:4]          # 4 位足够
        fut: asyncio.Future[str] = asyncio.Future()
        async with self._lock:
            self._pending_probe[uid] = fut

        # 发探针：服务器一般会回显“.[uid]”
        await fire(chat(f"{self._probe_prefix}{uid}"))

        try:
            channel = await asyncio.wait_for(fut, self._timeout)
            return channel
        except asyncio.TimeoutError:
            logger.warning(f"探针 {uid} 超时，用缓存 {self._current}")
            return self._current
        finally:
            async with self._lock:
                self._pending_probe.pop(uid, None)

    # -------- 工具：扫行首 --------
    def _parse_channel(self, content: str) -> str:
        for pattern, ch in _COMPILED.items():
            if pattern.search(content):
                return ch
        return self._current          # 没匹配就保持旧值
