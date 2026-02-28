import re

import cv2
import numpy as np
import pyautogui
import pytesseract

from ..constants import ROI, TESSERACT_CMD
from ..utils.logger import logger  # 引入日志
from .find_window import get_foreground_title
from .functools import sync_to_async

QUEUE_RE = re.compile(r"(\d+)\s*/\s*(\d+)")
pytesseract.pytesseract.tesseract_cmd = str(TESSERACT_CMD)

# 合理性边界（按服务器实际上限调）
_MAX_POP = 3000
_MIN_POP = 0


def ocr_available() -> bool:
    """ocr是否可用"""
    return TESSERACT_CMD.exists()


@sync_to_async
def _ocr_text() -> str:
    title = get_foreground_title()
    if title and "Minecraft" in title:
        # 1. 截图
        img_np = pyautogui.screenshot(region=ROI)
        # 2. 反二值化：白字黑底，抗锯齿→硬边缘
        gray = cv2.cvtColor(np.array(img_np), cv2.COLOR_RGB2GRAY)
        _, bw = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        # 3. 只认数字 & "/"
        text = pytesseract.image_to_string(bw, config=r"--psm 7")
        return text.strip()
    else:
        return ""


async def queue_pos() -> tuple[int, int]:
    """获取游戏排队位置"""
    raw = await _ocr_text()
    m = QUEUE_RE.search(raw)
    if not m:
        logger.debug(f"OCR 无匹配: {raw!r}")
        return 0, 0

    cur, tot = map(int, m.groups())
    # 宇宙数字过滤
    if not (_MIN_POP <= cur <= tot <= _MAX_POP):
        logger.warning(f"OCR 异常值: {cur}/{tot}，丢弃")
        return 0, 0
    logger.trace(f"OCR匹配：{cur}/{tot}")

    return cur, tot
