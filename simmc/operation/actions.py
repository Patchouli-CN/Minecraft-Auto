
import time

import pyautogui

from ..utils.find_window import get_foreground_title
from ..utils.functools import sync_to_async


@sync_to_async
def jump_action(times: int = 3, interval: float = 1.0) -> None:
    title = get_foreground_title()
    if title and "Minecraft" in title:
        for i in range(1, times + 1):
            pyautogui.keyDown("space")
            time.sleep(0.2)
            pyautogui.keyUp("space")
            if i < times + 1:
                time.sleep(interval)
