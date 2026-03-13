class SimmCFrameworkException(Exception):
    """框架异常基类"""


class ConfigFileError(SimmCFrameworkException):
    """配置文件出错"""


class ServerCommandError(SimmCFrameworkException):
    """指令不正确"""


class ListenerNotFoundError(SimmCFrameworkException):
    """监听器没找到"""
