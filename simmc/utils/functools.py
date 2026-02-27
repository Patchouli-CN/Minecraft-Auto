""" 函数工具 """

import asyncio
import atexit
import contextvars
import functools
from collections.abc import Callable, Coroutine
from concurrent.futures import Executor, ThreadPoolExecutor
from functools import partial
from typing import Optional, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")

_default_pool = ThreadPoolExecutor(thread_name_prefix="AsyncThread")
atexit.register(_default_pool.shutdown, wait=True)

def sync_to_async(
    fn: Callable[P, T],
    *,
    executor: Optional[Executor] = None,
    force_thread: bool = True,
) -> Callable[P, Coroutine[None, None, T]]:
    if asyncio.iscoroutinefunction(fn):
        raise TypeError("fn must be a synchronous callable")
    if executor is None:
        executor = _default_pool

    if not force_thread:                       # 快速路径
        @functools.wraps(fn)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            return fn(*args, **kwargs)
        return wrapper

    @functools.wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        func_invoke = partial(ctx.run, fn, *args, **kwargs)
        return await loop.run_in_executor(
            executor, func_invoke
        )

    # 补全 qualname
    wrapper.__qualname__ = getattr(fn, "__qualname__", fn.__name__)
    return wrapper
