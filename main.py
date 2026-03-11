
import asyncio
from simmc.schedule.default import EventLoopScheduler
from simmc.listeners.evt_listener import MinecraftLogListener
from simmc.schemas.event import WhisperEvent, KickEvent, ViewSyncEvent, DisconnectEvent, GameCrashedEvent, LandInviteEvent
from simmc.utils.logger import logger
from simmc.operation.fluent.base import fluent_init_control, jump, fluent_wait
from simmc.operation.fluent.command import chat
from simmc.operation.fluent.land import land
from simmc.operation.player_control import PlayerControl
from simmc.constants import MYSELF
from simmc.services.channel_ensure import ChannelEnsureService
from simmc.services.triggers import JsonTriggerService
from simmc.services.idle_service import IdleService

event_scheduler = EventLoopScheduler()
ctrl = PlayerControl()

fluent_init_control(ctrl)

event_scheduler.add_start_prepare(ctrl.start)
event_scheduler.add_exit_callback(ctrl.stop)

# 1. 添加监听器
event_scheduler.add_listener(MinecraftLogListener())

# 2. 添加服务
ce_svc = ChannelEnsureService(MYSELF, 5)
idle_svc = IdleService()
event_scheduler.add_service(ce_svc) # 挂载服务到总控，用于接收事件
event_scheduler.add_service(idle_svc)
event_scheduler.add_service(JsonTriggerService())

# 3. 注册事件回调
@event_scheduler.on_event("悄悄话")
async def on_whisper(ev: WhisperEvent) -> None:
    logger.info(f"悄悄话：{ev.sender} 悄悄对你说：{ev.content}")
    if "跳一跳" in ev.content or "在" in ev.content:
        logger.warning(f"检测到管理员 {ev.sender}, 疑似审查言论, 执行规避...")
        await (fluent_wait(2) >> jump(3).interval(1))
        # current_channel = await ce_svc.get_channel()
        # await chat(f"当前频道: {current_channel}").sendto("Kamishirasawa_CN")

@event_scheduler.on_event("踢出")
def on_tick(ev: KickEvent) -> None:
    """ 被踢出处理 """
    raise KeyboardInterrupt(f"啊！我被踢了，程序自动退出...")

@event_scheduler.on_event("视角同步")
async def on_view_changed(ev: ViewSyncEvent) -> None: 
    """ 如果视角被管理员同步，处理 """
    logger.warning(f"管理员: {ev.admin_name} 同步视角，发送假消息规避ing")
    await chat("干嘛干嘛，要干嘛...").send_to(ev.admin_name)

@event_scheduler.on_event("断开")
def on_disconnect(ev: DisconnectEvent) -> None:
    """ 日志出现断开字样，立即自杀 """
    raise KeyboardInterrupt(f"检测到断开，程序马上退出...")

@event_scheduler.on_event("领地邀请")
async def on_land_invite(ev: LandInviteEvent) -> None:
    """ 处理领地邀请 """
    await land(ev.land_name).invite.accept()
    # async with land(ev.land_name).edit() as editor:
    #     await editor.claim.erase.auto()

@event_scheduler.on_event("游戏崩溃")
def on_game_crashed(ev: GameCrashedEvent) -> None:
    logger.critical("MC 核心崩溃，框架立即退出！")
    raise KeyboardInterrupt("检测到游戏崩溃")

if __name__ == "__main__":
    asyncio.run(event_scheduler.loop())