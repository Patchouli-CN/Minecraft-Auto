"""java字节码类型解析器"""

from enum import Enum


class JvmType(Enum):
    VOID = ("V", "None")
    BOOLEAN = ("Z", "bool")
    BYTE = ("B", "int")
    CHAR = ("C", "str")  # Java char 是 16-bit，Python 用 str 表示单字符
    SHORT = ("S", "int")
    INT = ("I", "int")
    LONG = ("J", "int")
    FLOAT = ("F", "float")
    DOUBLE = ("D", "float")

    def __init__(self, desc: str, py_name: str):
        self.desc = desc
        self.py_name = py_name

    @classmethod
    def from_desc(cls, desc: str):
        for t in cls:
            if t.desc == desc:
                return t
        return None


def descriptor_to_pytype(desc: str, mapper=None) -> str:
    """将字段或返回类型描述符转为 Python 类型名（字符串）"""
    if not desc:
        return "object"

    # 基本类型
    jvm_type = JvmType.from_desc(desc)
    if jvm_type:
        return jvm_type.py_name

    if desc.startswith("L") and desc.endswith(";"):
        internal_name = desc[1:-1]  # e.g., "net/minecraft/class_123"

        # 如果是混淆类（包含 class_数字），尝试反混淆
        if "/class_" in internal_name and mapper is not None:
            dot_name = internal_name.replace("/", ".")
            deobfed = mapper.deobf_class(dot_name)
            return deobfed.split(".")[-1]  # 返回简单类名

        # 否则取简单名
        return internal_name.split("/")[-1]

    # 数组（简化为 list）
    if desc.startswith("["):
        return "list"
    elif desc == "[B":
        return "bytes"

    return "object"


def parse_method_params(desc: str, mapper=None) -> list[str]:
    """解析方法描述符中的参数类型列表（返回 Python 类型名列表）"""
    if not desc.startswith("("):
        raise ValueError(f"Invalid method descriptor: {desc}")
    end = desc.find(")")
    if end == -1:
        raise ValueError("Missing ')' in method descriptor")
    params_str = desc[1:end]
    types = []
    i = 0
    while i < len(params_str):
        c = params_str[i]
        if c in "ZBCSIJFD":
            types.append(descriptor_to_pytype(c, mapper=mapper))  # ← 传 mapper
            i += 1
        elif c == "L":
            end_class = params_str.find(";", i)
            if end_class == -1:
                raise ValueError("Unterminated class in descriptor")
            full_desc = params_str[i : end_class + 1]
            types.append(descriptor_to_pytype(full_desc, mapper=mapper))  # ← 传 mapper
            i = end_class + 1
        elif c == "[":
            depth = 0
            while i < len(params_str) and params_str[i] == "[":
                depth += 1
                i += 1
            if i >= len(params_str):
                raise ValueError("Invalid array in descriptor")
            base_c = params_str[i]
            if base_c == "L":
                end_class = params_str.find(";", i)
                i = end_class
            else:
                types.append("list")
            i += 1
        else:
            raise ValueError(f"Unknown type char: {c}")
    return types
