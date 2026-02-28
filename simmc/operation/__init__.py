"""
operation 子模块
提供对 Minecraft 客户端的“安全”输入接口，
所有函数均为异步封装，可在事件循环中直接调用。
"""

from .type_chat import type_in_chat

__all__ = ["type_in_chat"]
