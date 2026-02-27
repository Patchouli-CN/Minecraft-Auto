import sys
from typing import Optional

from .functools import sync_to_async

# ------------------------------------------------------------------
# Windows
# ------------------------------------------------------------------
if sys.platform == "win32":
    import win32con
    import win32gui

    def _find_mc_hwnd(title_sub: str = "Minecraft") -> Optional[int]:
        """同步：返回第一个匹配窗口句柄，找不到返回 None"""
        buf: list[int] = []

        def _enum_cb(hwnd: int, _) -> None:
            if win32gui.IsWindowVisible(hwnd):
                if title_sub.lower() in win32gui.GetWindowText(hwnd).lower():
                    buf.append(hwnd)

        win32gui.EnumWindows(_enum_cb, None)
        return buf[0] if buf else None

    @sync_to_async
    def _restore_and_foreground(hwnd: int) -> None:
        """同步：还原最小化并提到最前"""
        win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
        win32gui.SetForegroundWindow(hwnd)

    async def focus_mc_window(title: str = "Minecraft") -> bool:
        hwnd = _find_mc_hwnd(title)
        if hwnd is None:
            return False
        await _restore_and_foreground(hwnd)
        return True

# ------------------------------------------------------------------
# macOS
# ------------------------------------------------------------------
elif sys.platform == "Darwin":
    from AppKit import NSWorkspace

    @sync_to_async
    def _activate_app_by_name(name_sub: str) -> bool:
        """同步：在 runningApplications 里搜名字并激活，成功返回 True"""
        for app in NSWorkspace.sharedWorkspace().runningApplications():
            if name_sub.lower() in app.localizedName().lower():
                # 0 == NSApplicationActivateIgnoringOtherApps
                app.activateWithOptions_(0)
                return True
        return False

    async def focus_mc_window(title: str = "Minecraft") -> bool:
        return await _activate_app_by_name(title)

# ------------------------------------------------------------------
# Linux (X11 + wmctrl)
# ------------------------------------------------------------------
else:
    import asyncio
    import shlex

    async def focus_mc_window(title: str = "Minecraft") -> bool:
        cmd = f"wmctrl -a {shlex.quote(title)}"
        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.DEVNULL, stderr=asyncio.DEVNULL
        )
        return await proc.wait() == 0
