import time

import pyautogui
import pyperclip

from ..utils.functools import sync_to_async

# 防止 pyautogui 把鼠标飞走
pyautogui.FAILSAFE = True
pyautogui.PAUSE = 0.0  # 手动控制延迟，不用默认 sleep


@sync_to_async
def type_in_chat(text: str | list[str], interval: float = 1.0) -> None:
    """打字"""
    if isinstance(text, list):
        for _text in text:
            _type_in_chat(_text)
    else:
        _type_in_chat(text)
    time.sleep(interval)


def _type_in_chat(text: str) -> None:
    pyperclip.copy(text)
    pyautogui.press("enter")
    time.sleep(0.2)
    pyautogui.hotkey("ctrl", "v")
    time.sleep(0.1)
    pyautogui.press("enter")
