#!/usr/bin/env python3
"""
清掉项目下所有 __pycache__ 目录与 *.pyc / *.pyo 文件
用法：
    python clean_pycache.py          # 清当前目录
    python clean_pycache.py /path    # 清指定路径
"""

from pathlib import Path
import sys
import shutil


def clean(root: Path) -> None:
    """递归删除 __pycache__ 和 pyc/pyo"""
    removed = 0
    for pyc in root.rglob("*.pyc"):
        pyc.unlink(missing_ok=True)
        removed += 1
    for pyo in root.rglob("*.pyo"):
        pyo.unlink(missing_ok=True)
        removed += 1
    for cache_dir in root.rglob("__pycache__"):
        shutil.rmtree(cache_dir, ignore_errors=True)
        removed += 1
    print(f"✅ 已清理 {removed} 项来自 {root}")


if __name__ == "__main__":
    target = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path.cwd()
    if not target.exists():
        print(f"❌ 路径不存在：{target}")
        sys.exit(1)
    clean(target)
