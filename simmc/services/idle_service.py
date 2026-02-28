from ..operation.fluent.base import fluent_wait, jump
from ..schemas.event import PlayerIdleEvent, PlayerResumeEvent
from ..utils.logger import logger


class IdleService:
    """挂机服务"""

    def __init__(self):
        self.detected = False
        self._detected_nums = 0

    # --------- 统一的 IService 接口 ---------
    async def handle(self, ev: PlayerIdleEvent | PlayerResumeEvent) -> None:
        """调度器唯一入口：事件分派"""
        if isinstance(ev, PlayerIdleEvent):
            await self.handle_idle(ev)
        elif isinstance(ev, PlayerResumeEvent):
            self.handle_resume(ev)

    # --------- 原逻辑保持不变 ---------
    async def handle_idle(self, ev: PlayerIdleEvent) -> None:
        if self.detected:  # 防止重复协程
            return
        self.detected = True
        self._detected_nums += 1
        logger.info("服务器检测到我挂机了，跳一跳来避免被踢出...")
        while self.detected:
            await (jump(3).interval(1).timeout(4) >> fluent_wait(4))

    def handle_resume(self, ev: PlayerResumeEvent) -> None:
        if not self.detected:
            return
        self.detected = False
        logger.info(f"已经骗过服务器 {self._detected_nums} 次.")

    def detect_times(self) -> int:
        return self._detected_nums
