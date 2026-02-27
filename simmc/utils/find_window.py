
import sys
from typing import Optional

if sys.platform == "win32":
    import win32gui

    def get_foreground_title() -> Optional[str]:
        hwnd = win32gui.GetForegroundWindow()
        return win32gui.GetWindowText(hwnd) if hwnd else None

elif sys.platform == "Darwin":
    from AppKit import NSWorkspace

    def get_foreground_title() -> Optional[str]:
        app = NSWorkspace.sharedWorkspace().frontmostApplication()
        return app.localizedName() if app else None

else:  # Linux X11
    def get_foreground_title() -> Optional[str]:
        import subprocess
        cmd = "xdotool getwindowfocus getwindowname"
        try:
            return subprocess.check_output(cmd, shell=True, text=True).strip()
        except Exception:
            return None
