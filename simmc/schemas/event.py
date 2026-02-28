"""事件发生数据"""

from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar

from ..constants import CMD_CHANNEL_TABLE
from .event_registry import event

T = TypeVar("T")


class EventBase(Generic[T]):
    """事件基类"""


@dataclass(slots=True)
class EventRequest:
    """事件请求"""

    event_name: str
    """ 事件的名字"""
    event: EventBase
    """ 事件数据 """
    happen_time = datetime.now()
    """ 事件发生时间 """


@event("消息")
class MessageEvent(EventBase[str]):
    """消息事件"""

    def __init__(
        self,
        server_name: str = "主服",
        channel: str = "G",
        tag: str = "流浪者",
        player: str = "",
        content: str = "",
    ) -> None:
        self.server_name = server_name
        """ 玩家所在服务器区域 """
        self.channel = CMD_CHANNEL_TABLE.get(channel, "G")
        """ 玩家所在频道 """
        self.tag = tag
        """ 玩家标签 """
        self.player = player
        """ 玩家名字 """
        self.content = content
        """ 消息内容 """


@event("悄悄话")
class WhisperEvent(EventBase[str]):
    """悄悄话消息事件"""

    def __init__(self, sender: str, text: str) -> None:
        self.sender = sender
        """ 发送者 """
        self.text = text
        """ 消息内容 """

    def sender_is(self, name: str) -> bool:
        """发送者是不是 ..."""
        return self.sender == name

    @property
    def content(self) -> str:
        """消息内容"""
        return self.text


@event("加入")
class JoinEvent(EventBase[str]):
    """玩家加入事件"""

    def __init__(self, player: str) -> None:
        self.player = player


@event("退出")
class QuitEvent(EventBase[str]):
    """玩家退出事件"""

    def __init__(self, player: str) -> None:
        self.player = player


@event("踢出")
class KickEvent(EventBase[None]):
    """踢出事件"""


@event("视角变化")
class ViewForcedEvent(EventBase[int]):
    def __init__(self, dx: int, dy: int) -> None:
        self.dx = dx
        self.dy = dy


@event("视角同步")
class ViewSyncEvent(EventBase[str]):
    """视角同步事件"""

    def __init__(self, admin_name: str) -> None:
        self.admin_name = admin_name
        """ 哪个管理员同步了我的视角？ """


@event("断开")
class DisconnectEvent(EventBase[None]):
    """网络层断开事件"""


@event("领地邀请")
class LandInviteEvent(EventBase[str]):
    """领地邀请事件"""

    def __init__(self, inviter: str, land_name: str) -> None:
        self.inviter = inviter
        """ 邀请人 """
        self.land_name = land_name
        """ 领地名称 """


@event("挂机")
class PlayerIdleEvent(EventBase[None]):
    """服务器检测到玩家静止，自动标记为挂机"""


@event("挂机恢复")
class PlayerResumeEvent(EventBase[None]):
    """玩家恢复操作，服务器取消挂机标记"""


@event("领地存钱")
class LandDepositEvent(EventBase[str]):
    def __init__(self, land_name: str, player: str, in_value: str, now_value: str):
        self.land_name = land_name
        self.player = player
        self.in_value = float(in_value.replace(",", ""))
        self.now_value = now_value


@event("领地取钱")
class LandWithdrawEvent(EventBase[str]):
    def __init__(self, land_name: str, player: str, out_value: str, now_value: str):
        self.land_name = land_name
        self.player = player
        self.out_value = float(out_value.replace(",", ""))
        self.now_value = now_value


@event("游戏崩溃")
class GameCrashedEvent(EventBase[None]):
    """MC 进程自身抛出 FATAL / 生成 crash-report"""
