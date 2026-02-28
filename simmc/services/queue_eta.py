import time
from collections import deque
from datetime import timedelta
from typing import Optional


class QueueEtaService:
    """
    基于历史数据的双指标 ETA 预测器：
    - 服务时间 τ：每人平均耗时（秒）
    - 排队速率 r：队列每秒减少人数
    """

    def __init__(
        self,
        service_window_size: int = 20,  # 用于估计服务时间的历史出队人数
        drain_window_duration: float = 60.0,  # 用于估计排队速率的时间窗口（秒）
        min_drain_samples: int = 3,  # 排队速率至少需要多少次位置变化
        max_eta_sec: float = 7200,  # 最大 ETA（2小时）
        default_service_time: float = 2.0,  # 默认服务时间（秒/人）
    ):
        # === 服务时间估计（基于已出队玩家）===
        self.service_window_size = service_window_size
        self.service_times: deque[float] = deque(
            maxlen=service_window_size
        )  # 每人的实际服务时间
        self.last_dequeued: Optional[tuple[int, float]] = (
            None  # (position_before_dequeue, timestamp)
        )

        # === 排队速率估计（基于你的位置变化）===
        self.drain_window_duration = drain_window_duration
        self.position_history: deque[tuple[float, int]] = (
            deque()
        )  # (timestamp, position)

        # === 其他参数 ===
        self.min_drain_samples = min_drain_samples
        self.max_eta_sec = max_eta_sec
        self.default_service_time = default_service_time

    def refresh(self) -> None:
        """清空所有历史数据，重置状态（用于重连或异常恢复）"""
        self.service_times.clear()
        self.last_dequeued = None
        self.position_history.clear()

    def _update_service_time(self, current: int, now: float) -> None:
        """
        更新服务时间估计。
        原理：当有人出队（current 减少），说明前一个人刚被服务完。
        """
        if self.last_dequeued is not None:
            last_pos, last_ts = self.last_dequeued
            # 如果 current < last_pos，说明有人出队了
            if current < last_pos:
                # 假设每次只出队1人（Minecraft 通常如此）
                service_time = now - last_ts
                if service_time > 0:
                    self.service_times.append(service_time)
        self.last_dequeued = (current, now)

    def _estimate_service_time(self) -> float:
        """返回当前估计的平均服务时间（秒/人）"""
        if self.service_times:
            return sum(self.service_times) / len(self.service_times)
        return self.default_service_time

    def _estimate_drain_rate(self, current: int, now: float) -> Optional[float]:
        """
        返回队列排水速率 r（人/秒），即 |dq/dt|
        要求：窗口内至少有 min_drain_samples 次有效移动
        """
        # 清理过期数据
        while (
            self.position_history
            and now - self.position_history[0][0] > self.drain_window_duration
        ):
            self.position_history.popleft()

        self.position_history.append((now, current))

        if len(self.position_history) < 2:
            return None

        # 找出所有位置下降的点（避免 total 跳变干扰）
        valid_points = []
        last_q = float("inf")
        for ts, q in self.position_history:
            if q < last_q:  # 只记录单调下降的点
                valid_points.append((ts, q))
                last_q = q

        if len(valid_points) < self.min_drain_samples:
            return None

        first_ts, first_q = valid_points[0]
        last_ts, last_q = valid_points[-1]
        dt = last_ts - first_ts
        dq = first_q - last_q  # 出队人数

        if dt <= 0 or dq <= 0:
            return None

        return dq / dt  # 人/秒

    def look(
        self, current: int, utc_timestamp: float | None = None
    ) -> Optional[timedelta]:
        """
        输入当前队列位置和 UTC 时间戳，返回 ETA。
        - 若 current <= 0: 返回 0
        - 若检测到队列恶化: 返回 None（由调用方处理为警告）
        - 否则: 返回合理 ETA
        """
        if current <= 0:
            return timedelta(seconds=0)

        now = utc_timestamp or time.time()
        self._update_service_time(current, now)

        # 尝试用排水速率预测
        drain_rate = self._estimate_drain_rate(current, now)
        if drain_rate is not None and drain_rate > 0:
            eta_sec = current / drain_rate
            if 0 < eta_sec <= self.max_eta_sec:
                return timedelta(seconds=eta_sec)

        # fallback: 用服务时间估计
        tau = self._estimate_service_time()
        eta_sec = current * tau
        if 0 < eta_sec <= self.max_eta_sec:
            return timedelta(seconds=eta_sec)

        # 超出合理范围
        return None
