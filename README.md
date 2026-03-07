
# Minecraft Auto - 强大的我的世界服务器自动工具

## 1. 功能介绍 Functions

### 1. 自动运行 SimMC 相关玩法 SimMC Automatic

  - 本项目主要为**SimMC**服务器服务，因此对**SimMC**适配良好。
    1. 防挂机踢出
    2. 防管理员跟踪视野
    3. 操作领地
    4. 转账
    
### 2. 友好的自定义功能，详见下方。 Friendly Customizations

### 3. 开源项目，可自行更改源代码。 Code Change Supports

## 2. 使用方法 Usage

### 1. 下载 Installation

- 首先， 使用Git下载此项目。

   ```bash
   git clone https://github.com/Patchouli-CN/Minecraft-Auto.git
   ```
- 之后下载Python 3.12及以上版本。

[![Python](https://img.shields.io/badge/Python-3.12.10-3776AB?style=flat&logo=python&logoColor=white&labelColor=444444)](https://www.python.org/downloads/release/python-31210/)

### 2. 配置 Configuration
- 首先运行`main.py`，会生成两个文件
`patten.json` 和 `config.json`，打开`config.json`。
 - 找到`MINECRAFT`区域，把`USER_ID`换成自己的玩家ID。
 例如：
   ```JSON
   "MINECRAFT": {
      "GAME_TITLE": "Minecraft* 1.21.8 - 多人游戏（第三方服务器）", # 这里是你的游戏窗口标题
      "USER_ID": "Steve"
   },
   ```
 - 找到`MinecraftLogListener`，将`log_path`换成自己的`latest.log`路径 (通常在`.minecraft`文件夹中的`log`文件夹中)。
 - 将`MinecraftLogListener`中的`mode`改为`"file"`。
 - 再次启动`main.py`文件，完成配置。
 - 如果你想程序运行的更快，建议把 `mode` 设置为 `socket` 模式。
   - 在`myagent`文件夹中找到`mc-agent.jar`。
   - 退出游戏，然后点开PCL -> 版本设置 -> 设置 -> 高级 -> Java虚拟机参数。(其他启动器同理)
   - 把下列字符串复制进去，记得要替换成上述`mc-agent.jar`路径
     ```json
     -javaagent:"你的mc-agent.jar路径"
     ```
   - 再次启动游戏，随后启动程序，完成配置。

### 3. 自定义 Costomization
 - 一般情况下, 运行`main.py`将会启动默认决策，此时**Minecraft-Auto**默认适配我的世界**SimMC服务器**。
 - 若想自定义化脚本，可以在`config.json`中加入`triggers`字段，如下：
```json
 "triggers": [
   {
    "comment": "管理员私聊说“V我100”就真给他100",
    "on": "悄悄话",
    "when": { "sender": "Kamishirasawa_CN", "text": "re:V我100" },
    "do": {
      "cmd": "pay",
      "args": { "amount": 100 },
      "chain": [["transfer_to", "Kamishirasawa_CN"]]
    }
  },
  {
    "comment": "收到任意领地邀请就自动接受",
    "on": "领地邀请",
    "when": {},
    "do": {
      "cmd": "land",
      "args": {},
      "chain": [["handle_invite"], ["accept"]]
    }
  }
 ]
```
 具体的`triggers`内容可见[TRIGGERS.md](TRIGGERS.md)内部介绍。

###


