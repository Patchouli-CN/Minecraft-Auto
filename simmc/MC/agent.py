from typing import Any, Protocol, cast

from py4j.java_gateway import JavaClass, JavaGateway, JavaObject

from ..utils.tiny_mapper import TinyMapper

mapper = TinyMapper(r"D:\python_play\SIMMC_proj\myagent\mappings\mappings.tiny")

class MCAgent(Protocol):
    """Minecraft 动态反射入口点（客户端/服务端通用）"""

    # ========== 类加载 ==========
    def refreshClassCache(self) -> None:
        """ 刷新agent类缓存，避免由于启动过早而找不到类 """
        ...

    def getAllClassesName(self) -> list[str]:
        """获取所有已加载的类名（混淆名，点号格式）"""
        ...

    def getClass(self, name: str) -> JavaClass:
        """通过类名（混淆名）获取 Class<?> 对象"""
        ...

    def getClassName(self, obj: JavaObject) -> str:
        """获取对象的实际类名（混淆名）"""
        ...

    # ========== 方法调用 ==========
    def invokeStaticMethod(self, className: str, methodName: str, *args: Any) -> JavaObject:
        """调用静态方法"""
        ...

    def invokeStaticMethodNoArgs(self, className: str, methodName: str) -> JavaObject:
        """ 无参调用静态方法 """
        ...

    def invokeMethod(self, obj: JavaObject, methodName: str, *args: Any) -> JavaObject:
        """调用实例方法"""
        ...

    def invokeMethodNoArgs(self, obj: JavaObject, methodName: str) -> JavaObject:
        """ 无参调用实例方法 """
        ...

    # ========== 字段读写 ==========
    def readStaticField(self, className: str, fieldName: str) -> JavaObject:
        """读取静态字段"""
        ...

    def readField(self, obj: JavaObject, fieldName: str) -> JavaObject:
        """读取实例字段"""
        ...

    def writeField(self, obj: JavaObject, fieldName: str, value: Any) -> None:
        """写入实例字段"""
        ...

    def writeStaticField(self, className: str, fieldName: str, value: Any) -> None:
        """写入静态字段"""
        ...

    # ========== 方法列表（用于探索）==========
    def getPublicMethods(self, className: str) -> list[str]:
        """获取类的所有 public 方法签名（字符串形式）"""
        ...

    def getDeclaredMethods(self, className: str) -> list[str]:
        """获取类的所有 declared 方法签名（包括 private）"""
        ...

gateway = JavaGateway(auto_convert=True)

def get_agent() -> MCAgent:
    return cast(MCAgent, gateway)

entry = get_agent()

MinecraftClient = entry.invokeStaticMethodNoArgs("net.minecraft.class_310", "method_1551") # MinecraftClient.getInstance()
"""
MC客户端实例
"""
