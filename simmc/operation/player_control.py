import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Optional

from ..utils.logger import logger

# 回调签名：async fn(**kw) -> None
Handler = Callable[..., Awaitable[None]]

@dataclass(slots=True)
class PlayerOperationRequest:
    handler: Handler
    kw: dict
    future: asyncio.Future[None]

class PlayerControl:
    """玩家身体独占控制器：天然协程安全、自带排队"""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[PlayerOperationRequest] = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    # ------------------ 公共 API ------------------
    async def request(
        self,
        handler: Handler,
        **kw,
    ) -> asyncio.Future[None]:
        """
        排队申请玩家控制权
        :param handler: 真正需要动键鼠的协程函数
        :param timeout: 排队+执行最大等待时间
        :param kw: 传给 handler 的参数
        """
        fut: asyncio.Future[None] = asyncio.Future()
        req = PlayerOperationRequest(handler, kw, fut)
        await self._queue.put(req)
        return fut

    # ------------------ 生命周期 ------------------
    def start(self) -> None:
        """在调度器启动时调用一次即可"""
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker())

    def stop(self) -> None:
        if self._worker_task is not None:
            self._worker_task.cancel()
            self._worker_task = None

    # ------------------ 内部 worker ------------------
    async def _worker(self) -> None:
        logger.info("玩家控制器启动")
        while True:
            req = await self._queue.get()
            try:
                logger.debug(f"执行玩家控制请求：{req.handler!r}")
                async with self._lock:
                    await req.handler(**req.kw)          # 关键区
            except Exception as exc:
                logger.exception(f"玩家控制请求执行失败：{exc}")
                if not req.future.done():
                    req.future.set_exception(exc)
            else:
                if not req.future.done():
                    req.future.set_result(None)
            finally:
                self._queue.task_done()
