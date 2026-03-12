"""默认调度器"""

import asyncio
import inspect
import typing
from asyncio import Task
from collections.abc import Awaitable, Callable, Coroutine
from datetime import timedelta
from time import monotonic
from typing import Any, Self, TypeVar

from ..constants import BANNER
from ..metadata import print_banner
from ..schemas.event import EventBase, EventRequest
from ..schemas.protocols import IListener, IService
from ..utils.utils_functools import sync_to_async
from ..utils.logger import logger

EVENT = TypeVar("EVENT", bound=EventBase, covariant=True)
Handler = Callable[[EVENT], None] | Callable[[EVENT], Awaitable[None]]


class EventLoopScheduler:
    """事件调度器"""

    def __init__(self) -> None:
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._listeners: list[IListener] = []
        self._services: list[IService] = []
        self._handlers: dict[str, list[Callable]] = {}
        self._tasks: list[Task[None]] = []
        self._coro_buffer: list[Coroutine[Any, Any, None]] = []
        self._exit_callbacks: list[Callable[..., None]] = []
        self._startup_prepares: list[Callable[..., None]] = []
        self._svc_routes: list[tuple[IService, tuple[type, ...]]] = []
        self._runner: asyncio.Task | None = None
        self._start_mono: float = monotonic()

        self._exit_flag: bool = False

    def show_info(self) -> None:
        """显示信息"""
        print(BANNER)
        print_banner()

    def _extract_event_types(self, svc: IService) -> tuple[type, ...]:
        """获取服务类型"""
        sig = inspect.signature(svc.handle)
        param = next(iter(sig.parameters.values()), None)
        if param is None:
            return (EventBase,)  # 无参默认全收

        wanted_type = param.annotation
        if wanted_type is inspect.Parameter.empty:
            return (EventBase,)

        # Union 展开
        origin, args = typing.get_origin(wanted_type), typing.get_args(wanted_type)
        if origin is typing.Union:
            return tuple(args)
        return (wanted_type,)

    def get_running_time(self) -> timedelta:
        """获取调度器运行时间"""
        return timedelta(seconds=monotonic() - self._start_mono)

    def ensure_loop(self) -> None:
        """确认loop, 没有就new一个"""
        try:
            self._event_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._event_loop)

    def add_listener(self, listener: IListener) -> None:
        """添加监听器"""
        logger.debug(f"添加监听器：{type(listener).__name__}")
        self._listeners.append(listener)

    def add_service(self, svc: IService) -> None:
        """添加服务"""
        self._services.append(svc)
        wanted_types = self._extract_event_types(svc)
        self._svc_routes.append((svc, wanted_types))
        logger.debug(
            f"服务 {type(svc).__name__} 注册事件类型 {wanted_types}; 描述: {type(svc).__doc__}"
        )

    def add_start_prepare(self, start_prepare_function: Callable[..., None]) -> None:
        """添加启动前置执行"""
        logger.debug(
            f"添加启动前置：{start_prepare_function.__name__}; 描述: {start_prepare_function.__doc__}"
        )
        self._startup_prepares.append(start_prepare_function)

    def add_runtime_entrust(
        self, entrust_in_runtime: Coroutine[Any, Any, None]
    ) -> None:
        """
        委托你的携程给调度器，让它再运行时执行
        """
        if self._event_loop and self._event_loop.is_running():
            self._to_runtime_task(entrust_in_runtime)
        else:
            self._coro_buffer.append(entrust_in_runtime)

    def _to_runtime_task(self, task_in_runtime: Coroutine[Any, Any, None]) -> None:
        """运行时任务"""
        self._tasks.append(asyncio.create_task(task_in_runtime))

    def add_exit_callback(self, exit_callback_function: Callable[..., None]) -> None:
        """添加退出回调"""
        logger.debug(
            f"添加退出回调：{exit_callback_function.__name__}; 描述: {exit_callback_function.__doc__}"
        )
        self._exit_callbacks.append(exit_callback_function)

    def on_event(self, name: str):
        """订阅一个事件，触发将取决于你加入的监听器发布事件的名字"""

        def event_decorator(fn: Handler) -> Handler:
            logger.debug(
                f"注册事件处理函数: {name} -> ({fn.__name__}) 描述: {fn.__doc__}"
            )
            self._handlers.setdefault(name, []).append(fn)
            return fn

        return event_decorator

    async def loop(self) -> None:
        """主循环"""
        self.show_info()
        logger.info("开始运行...")
        self.ensure_loop()
        # 执行启动前置
        for prepare in self._startup_prepares:
            try:
                if inspect.iscoroutinefunction(prepare):
                    await prepare()
                else:
                    prepare()
            except Exception as prepare_exc:
                logger.warning(
                    f"执行前置 {prepare.__name__} 出错: {prepare_exc!r}:{prepare_exc}"
                )

        # 运行时委托转任务委托给asyncio-loop
        for coro in self._coro_buffer:
            self._to_runtime_task(coro)

        if not self._listeners:
            raise RuntimeError("没有监听器，请先 add_listener()")

        # 启动所有 listener 协程
        for listener in self._listeners:
            self._to_runtime_task(self._pump(listener))

        try:
            await self._loop()
        except asyncio.CancelledError:
            await self.stop()

    async def _loop(self):
        return await asyncio.gather(*self._tasks)

    async def stop(self) -> None:
        """停止循环"""
        if not self._exit_flag:
            self._exit_flag = True  # 确认退出状态
            logger.info("正在退出并执行回调...")
            for t in self._tasks:
                t.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)

            self._do_exit_callback()  # 执行退出回调

    def _do_exit_callback(self) -> None:
        """执行退出回调"""
        if len(self._exit_callbacks) == 0:
            logger.info("没有任何退出回调被注册，程序正常退出...")
            return
        for callback in self._exit_callbacks:
            try:
                callback()
                logger.success(f"执行回调: {callback.__name__} 成功")
            except Exception:
                logger.warning(f"回调 {callback.__name__} 执行失败")
                continue
        logger.success("所有回调执行完成，正式退出...")

    # ------------------------
    async def _pump(self, listener: IListener) -> None:
        async for ev in listener.listen():
            self._dispatch(ev)

    def _dispatch(self, ev: EventRequest) -> None:
        """发布事件"""
        self.__to_service(ev)
        self.__to_decorator(ev)

    def __to_service(self, ev: EventRequest) -> None:
        """发布事件给服务"""
        for svc, types in self._svc_routes:
            if any(isinstance(ev.event, t) for t in types):
                coro = svc.handle(ev.event)
                task = asyncio.create_task(coro, name=ev.event_name)
                task.add_done_callback(self._handle_task_exc)

    def __to_decorator(self, ev: EventRequest) -> None:
        """发给业务装饰器"""
        for handler in self._handlers.get(ev.event_name, []):
            logger.trace(
                f"发布事件处理请求 {ev.event_name} 给业务装饰器: -> {handler.__name__}"
            )
            coro = (
                handler(ev.event)
                if inspect.iscoroutinefunction(handler)
                else sync_to_async(handler)(ev.event)
            )
            task = asyncio.create_task(coro, name=ev.event_name)
            # 关键：任务结束后统一检查异常
            task.add_done_callback(self._handle_task_exc)

    def _handle_task_exc(self, t: asyncio.Task) -> None:
        exc = t.exception()
        if exc:
            logger.error(
                f"处理事件: {t.get_name()} 的时候发生了异常: {type(exc).__name__}: {exc}",
                exc_info=exc,
            )
        if isinstance(exc, KeyboardInterrupt):
            logger.critical(f"收到处理函数传来的退出信号，正在退出，原因: {exc}")
            # 注意：必须在循环里创建 stop 任务，否则死锁
            asyncio.create_task(self.stop())

    async def __aenter__(self) -> Self:
        # 启动调度器，但不要把 run() 直接 await 死
        self._runner = asyncio.create_task(self.loop())
        # 让调度器有机会初始化完（非必须，保险写法）
        await asyncio.sleep(0)
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()
        if self._runner and not self._runner.done():
            await self._runner
