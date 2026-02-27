
from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import partial
from typing import Literal, Optional, Self

from ...exceptions import ServerCommandError
from ...security import json_export
from ..type_chat import type_in_chat
from .base import FluentBase, ServerCommand


# 1. 编辑上下文对象，仅负责“构造”命令，不直接动键鼠
class _EditCtx(FluentBase):
    def __init__(self, land_name: str) -> None:
        self._land = land_name

    @property
    @json_export
    def trust(self) -> TrustFluent:
        """ 让此领地信任/失信一个玩家 """
        return TrustFluent()
    
    @property
    @json_export
    def claim(self) -> ClaimFluent:
        """ 领地圈地指令 """
        return ClaimFluent()

# 2. 具体子命令，依旧走 FluentBase → PlayerControl
class TrustFluent(ServerCommand, FluentBase):
    def __init__(self) -> None:
        self._cmd = ""

    @json_export
    def add(self, player: str) -> Self:
        """ 信任一个玩家 """
        self._cmd = f"/land trust {player}"
        return self
    
    @json_export
    def remove(self, player: str) -> Self:
        """ 失信一个玩家 """
        self._cmd = f"/land untrust {player}"
        return self

    def _build_handler(self) -> Callable[..., Awaitable[None]]:
        if self._check_null(self._cmd):
            raise ServerCommandError("trust子指令操作为空，不能啥也不做")
        return partial(type_in_chat, self._cmd, 0.0)

class ClaimFluent(FluentBase):
    def __init__(self) -> None:
        self._sub_op: Literal["radius", "auto", "fill"] = "auto"
        self._cmd = ""
        self._mode: Literal["claim", "unclaim"] = "claim"

    @property
    @json_export
    def draw(self) -> Self:
        """ 圈地模式 """
        self._mode = "claim"
        return self
    
    @property
    @json_export
    def erase(self) -> Self:
        """ 擦除模式 """
        self._mode = "unclaim"
        return self
    
    @json_export
    def radius(self, value: int) -> Self:
        """ 根据半径 """
        self._sub_op = "radius"
        self._radius_value = value
        return self
    
    @json_export
    def auto(self) -> Self:
        """ 自动 """
        self._sub_op = "auto"
        return self
    
    @json_export
    def fill(self) -> Self:
        """ 填充 """
        self._sub_op = "fill"
        return self
    
    def _handle_draw(self) -> None:
        if self._sub_op == "radius":
            self._cmd = f"/land {self._mode} {self._sub_op} {self._radius_value}"
        else:
            self._cmd = f"/land {self._mode} {self._sub_op}"

    def _handle_erase(self) -> None:
        if self._sub_op == "radius":
            self._cmd = f"/land {self._mode} {self._sub_op} {self._radius_value} confirm"
        else:
            self._cmd = f"/land {self._mode} {self._sub_op} confirm"

    def _build_handler(self) -> Callable[..., Awaitable[None]]:
        if self._mode == "claim":
            self._handle_draw()
        else:
            self._handle_erase()
        return partial(type_in_chat, self._cmd, 0.0)
    
class _EnterEditFluent(FluentBase):
    def __init__(self, land: str) -> None:
        self._cmd = f"/land edit {land}"

    def _build_handler(self) -> Callable[..., Awaitable[None]]:
        return partial(type_in_chat, self._cmd, 0.0)

class LandFluent(ServerCommand, FluentBase):
    """领地相关流式命令：邀请、存款、取款……"""

    def __init__(self, land_name: str) -> None:
        if not land_name or land_name.strip() == "":
            raise ValueError("land_name 不能为空")
        self._in_edit: bool = False
        self._land: str = land_name.strip()
        self._sub_command: str = ""        #  invite | deposit | withdraw …
        self._operation: Optional[str] = None   # accept | deny | all …

    # ---------- 子命令：入口 ----------
    @property
    @json_export
    def invite(self) -> LandFluent:
        """处理别人发给我的领地邀请"""
        self._sub_command = "invite"
        return self
    
    @json_export
    def deposit(self, value: str | int = "all") -> LandFluent:
        """向领地存钱"""
        self._sub_command = "deposit"
        self._value: str = str(value).lower()
        return self
    
    @asynccontextmanager
    @json_export
    async def edit(self, land_name: str | None = None) -> AsyncGenerator[_EditCtx, None]:
        # 进入编辑模式
        try:
            if self._in_edit:
                raise RuntimeError("已经进入编辑模式")
            self._in_edit = True
            await _EnterEditFluent(land_name or self._land)
            ctx = _EditCtx(land_name or self._land)
            yield ctx
        finally:
            self._in_edit = False

    # ---------- 操作：动词 ----------
    @json_export
    def accept(self) -> LandFluent:
        if self._sub_command != "invite":
            raise RuntimeError("只有 invite 后才能 accept()")
        self._operation = "accept"
        return self
    
    @json_export
    def reject(self) -> LandFluent:
        if self._sub_command != "invite":
            raise RuntimeError("只有 invite 后才能 reject()")
        self._operation = "deny"
        return self

    # ---------- 内部：生成协程 ----------
    def _build_handler(self) -> Callable[..., Awaitable[None]]:
        if self._sub_command == "invite":
            cmd = f"/land {self._operation} {self._land}"
        elif self._sub_command == "deposit":
            cmd = f"/land deposit {self._value} {self._land}"
        else:
            raise RuntimeError("子命令未设置，无法构建 handler")
        return partial(type_in_chat, cmd, 0.0)
    
def land(name: str) -> LandFluent:
    return LandFluent(name)
