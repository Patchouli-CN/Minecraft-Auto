""" 日志实现 """

import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

logger.remove()

# 配置日志文件路径
log_dir = Path("logs")
if not log_dir.exists():
    log_dir.mkdir()

# 定义日志文件名
log_file_name = f"runtime_{datetime.now().strftime('%Y-%m-%d')}.log"
log_file_path = log_dir / log_file_name

# 配置 loguru 日志
FILEHANDLER = logger.add(
    log_file_path,
    format="{time:HH:mm:ss} | {level:<8} | {thread.name:<12} / {name} | {function}:{line:03d} | {message}",
    level="DEBUG",
    rotation="1 week",
    compression="zip",
    backtrace=True,
    diagnose=True,
    enqueue=True
)

# 添加控制台日志输出
CONSOLEHANDLER = logger.add(
    sys.stderr,
    format="<level>{time:HH:mm:ss} | {level:<8} | {thread.name:<12} / {name} | {function}:{line:03d} | {message}</level>",
    level="TRACE",
    colorize=True
)

# 测试日志
if __name__ == "__main__":
    logger.trace("这是一个 trace 级别的日志")
    logger.debug("这是一个 debug 级别的日志")
    logger.info("这是一个 info 级别的日志")
    logger.success("这是一个 success 级别的日志")
    logger.warning("这是一个 warning 级别的日志")
    logger.error("这是一个 error 级别的日志")
    logger.critical("这是一个 critical 级别的日志")
