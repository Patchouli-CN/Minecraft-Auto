""" 常量 """
import json
import sys
from pathlib import Path
from typing import Any

from .exceptions import ConfigFileError
from .schemas.typing import EventRegexRules

BANNER = r"""
 ____  _                      ____        _         _
/ ___|(_)_ __ ___  _ __ ___  / ___|      / \  _   _| |_ ___
\___ \| | '_ ` _ \| '_ ` _ \| |   _____ / _ \| | | | __/ _ \
 ___) | | | | | | | | | | | | |__|_____/ ___ \ |_| | || (_) |
|____/|_|_| |_| |_|_| |_| |_|\____|   /_/   \_\__,_|\__\___/
"""

if getattr(sys, "frozen", False):
    # 打包后 exe 所在目录
    _BASE = Path(sys.executable)
else:
    # 源码目录
    _BASE = Path(__file__).parent

CONFIG_FILE = _BASE.with_name("config.json")
PATTERN_FILE = _BASE.with_name("patten.json")

_DEFAULT_PATTEN_TEMP: list[EventRegexRules] = \
[
  {
    "name": "游戏崩溃",
    "rules": [
      {
        "regex": "\\[\\d{2}:\\d{2}:\\d{2}\\] \\[.*FATAL\\]:.*",
        "groups": []
      },
      {
        "regex": ".*(Exception in server tick loop|This crash report has been saved to|---- Minecraft Crash Report ----).*",
        "groups": []
      }
    ]
  },
  {
    "name": "领地邀请",
    "rules": [
      {
        "regex": ".*?(?P<inviter>[\\w\u4e00-\u9fff]{2,16}).*?邀请.*?加入.*?(?P<land_name>[\\w\u4e00-\u9fff\\s·\\-]{2,32}).*?领土",
        "groups": [
          "inviter",
          "land_name"
        ]
      }
    ]
  },
  {
    "name": "领地存钱",
    "rules": [
      {
        "regex": "^\\[CHAT\\]\\s+领土>>\\s+收件箱\\s-\\s领土\\s+(?P<land_name>[^:]+):\\s+玩家\\s+(?P<player>\\w+)\\s+存入了\\s+\\$(?P<in_value>[\\d,]+\\.\\d{2})\\.当前余额:\\s+\\$(?P<now_value>[\\d,]+\\.\\d{2})$",
        "groups": [
          "land_name",
          "player",
          "in_value",
          "now_value"
        ]
      }
    ]
  },
  {
    "name": "领地取钱",
    "rules": [
      {
        "regex": "^\\[CHAT\\]\\s+领土>>\\s+收件箱\\s-\\s领土\\s+(?P<land_name>[^:]+):\\s+玩家\\s+(?P<player>\\w+)\\s+取出了\\s+\\$(?P<out_value>[\\d,]+\\.\\d{2})\\.当前余额:\\s+\\$(?P<now_value>[\\d,]+\\.\\d{2})$",
        "groups": [
          "land_name",
          "player",
          "out_value",
          "now_value"
        ]
      }
    ]
  },
  {
    "name": "悄悄话",
    "rules": [
      {
        "regex": "(?P<sender>\\w+) 悄悄的对 我 说: (?P<text>.+)",
        "groups": [
          "sender",
          "text"
        ]
      }
    ]
  },
  {
    "name": "消息",
    "rules": [
      {
        "regex": "\\[CHAT\\]\\s*?(?:\\[(?P<server_name>[^\\]]+?)\\]\\s*)?\\[(?P<channel>G|L|交易|RP|国家|[A-Z]{2,})\\]\\s+(?:(?P<tag>[^\\s:]+)\\s+)?(?P<player>[A-Za-z0-9_]{3,16})(?:\\s*[:>]|\\s+说)\\s*(?P<content>.*)$",
        "groups": [
          "server_name",
          "channel",
          "tag",
          "player",
          "content"
        ]
      }
    ]
  },
  {
    "name": "视角同步",
    "rules": [
      {
        "regex": "你的视角已与 (?P<admin_name>\\w+) 同步",
        "groups": [
          "admin_name"
        ]
      }
    ]
  },
  {
    "name": "加入",
    "rules": [
      {
        "regex": "(?P<player>\\w+) joined the game",
        "groups": [
          "player"
        ]
      },
      {
        "regex": "(?P<player>\\w+) 加入了游戏",
        "groups": [
          "player"
        ]
      }
    ]
  },
  {
    "name": "退出",
    "rules": [
      {
        "regex": "(?P<player>\\w+) left the game",
        "groups": [
          "player"
        ]
      },
      {
        "regex": "(?P<player>\\w+) 退出了游戏",
        "groups": [
          "player"
        ]
      }
    ]
  },
  {
    "name": "踢出",
    "rules": [
      {
        "regex": "您已被踢出",
        "groups": []
      }
    ]
  },
  {
    "name": "断开",
    "rules": [
      {
        "regex": ".*(?:Disconnected|连接断开|Timed out|Connection reset|lost connection).*",
        "groups": []
      }
    ]
  },
  {
    "name": "挂机",
    "rules": [
      {
        "regex": "你暂时离开了",
        "groups": []
      }
    ]
  },
  {
    "name": "挂机恢复",
    "rules": [
      {
        "regex": "你回来了",
        "groups": []
      }
    ]
  }
]

# 默认模板（第一次自动释放）
_DEFAULT_CONF = {
    "MINECRAFT": {
        "GAME_TITLE": "Minecraft* 1.21.8 - 多人游戏（第三方服务器）",
        "USER_ID": "Kamishirasawa_CN"
    },
    "QUEUE_OCR_SETTINGS": {
        "TESSERACT_CMD": "./tesseract/tesseract.exe",
        "ROI": [754, 894, 398, 23]
    },
    "SERVER": {
        "CHAT_CHANNELS": {
            "交易": "tc",
            "G": "global",
            "L": "local",
            "国家": "nations chat",
            "RP": "rp"
        }
    }
}

def _cfg_init() -> dict[str, Any]:
    try:
      if not PATTERN_FILE.exists():
          PATTERN_FILE.write_text(json.dumps(_DEFAULT_PATTEN_TEMP, ensure_ascii=False, indent=2), encoding="utf-8")

      if CONFIG_FILE.exists():
          return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
      # 自动生成默认
      CONFIG_FILE.write_text(json.dumps(_DEFAULT_CONF, ensure_ascii=False, indent=2), encoding="utf-8")
      return _DEFAULT_CONF
    except json.JSONDecodeError as cause:
        raise ConfigFileError("配置文件发生错误") from cause

_cfg = _cfg_init()

_pat: list[EventRegexRules] = json.loads(PATTERN_FILE.read_text(encoding="utf-8"))

# ---------- 结构化映射 ----------
MYSELF: str               = _cfg["MINECRAFT"]["USER_ID"]
GAME_TITLE: str           = _cfg["MINECRAFT"]["GAME_TITLE"]
TESSERACT_CMD: Path        = Path(_cfg["QUEUE_OCR_SETTINGS"]["TESSERACT_CMD"])
ROI: tuple                  = tuple(_cfg["QUEUE_OCR_SETTINGS"]["ROI"])

CMD_CHANNEL_TABLE: dict[str, str]    = _cfg["SERVER"]["CHAT_CHANNELS"]
PATTERNS             = _pat          # 正则表
TRIGGERS: list[dict] = _cfg.get("TRIGGERS", [])
