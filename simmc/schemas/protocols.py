"""协议规范"""

# simmc/schemas/interfaces.py
from collections.abc import AsyncGenerator
from typing import Protocol, TypeVar

from .event import EventBase, EventRequest

TEVENT = TypeVar("TEVENT", bound=EventBase, contravariant=True)


class IListener(Protocol):
    def listen(self) -> AsyncGenerator[EventRequest, None]: ...


class IService(Protocol[TEVENT]):
    async def handle(self, ev: TEVENT) -> None: ...
