"""mapping.tiny 解析器"""

import re
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache

from ..utils.jtype_parser import descriptor_to_pytype, parse_method_params

MAPPER: "TinyMapper | None" = None


@dataclass(slots=True)
class FieldInfo:
    jtype: str  # Java field descriptor (e.g., "I", "Ljava/lang/String;")
    obf_name: str  # intermediary name (e.g., "field_1742")
    real_name: str  # readable Yarn name (e.g., "options")

    @property
    def py_type(self) -> str:
        """返回该字段的 Python 类型名（用于显示或注解）"""
        return descriptor_to_pytype(self.jtype, MAPPER)

    def to_pysig(self) -> str:
        """返回类似 Python 字段声明的字符串"""
        return f"{self.real_name}: {self.py_type}"


@dataclass(slots=True)
class MethodInfo:
    desc: str  # method descriptor (e.g., "(I)V")
    obf_name: str  # intermediary name (e.g., "method_5678")
    real_name: str  # readable Yarn name (e.g., "run")
    params: dict[int, str]

    def to_pysig(self, use_arg_names: bool = True) -> str:
        """返回类似 Python 函数签名的字符串。"""
        param_types = parse_method_params(self.desc, MAPPER)
        return_type_desc = self.desc[self.desc.rfind(")") + 1 :]
        return_type = descriptor_to_pytype(return_type_desc, MAPPER)

        args = []

        for i, typ in enumerate(param_types):
            if use_arg_names and i in self.params:
                arg_str = f"{self.params[i]}: {typ}"
            else:
                arg_str = f"arg{i}: {typ}" if use_arg_names else typ
            args.append(arg_str)

        args_str = ", ".join(args)
        func_name = self.real_name if self.real_name != "<init>" else "__init__"
        return f"{func_name}({args_str}) -> {return_type}"


@dataclass(slots=True)
class MappedClassInfo:
    class_name: str  # readable class name (dot format)
    obf_class_name: str  # input obfuscated class name (dot format)
    methods: list[MethodInfo]
    fields: list[FieldInfo]


class TinyMapper:
    """mappings.tiny解析器"""

    def __init__(self, tiny_path: str):
        self.res_path = tiny_path
        self._inter_to_named: dict[str, str] = {}  # inter → named (slash format)
        self._named_to_inter: dict[str, str] = {}

        self._simple_to_inter: dict[str, list[str]] = defaultdict(
            list
        )  # simple → [full_inter1, full_inter2]
        self._simple_to_named: dict[str, list[str]] = defaultdict(
            list
        )  # simple → [full_named1, ...]

        # Pre-built per-class indexes for fast lookup
        self._class_methods: defaultdict[str, list[MethodInfo]] = defaultdict(list)
        self._class_fields: defaultdict[str, list[FieldInfo]] = defaultdict(list)

        self.parse()
        self._named_to_inter = self._reverse_dict(self._inter_to_named)

    def parse(self) -> None:
        """解析 mappings.tiny 文件"""
        global MAPPER
        with open(self.res_path, encoding="utf-8") as f:
            lines = f.readlines()

        if not lines or not lines[0].startswith("tiny\t2"):
            raise ValueError("Not a valid Tiny v2 file")

        current_class_inter = None
        current_method_obf = None

        for line in lines[1:]:
            line = line.rstrip("\n")
            if not line:
                continue

            parts = line.split("\t")
            prefix = parts[0]

            # 处理类行: "c\tinter\tname"
            if prefix == "c":
                if len(parts) >= 3:
                    current_class_inter = parts[1]
                    self._inter_to_named[current_class_inter] = parts[2]
                    current_method_obf = None
                continue

            # 必须在类内
            if current_class_inter is None:
                continue

            # 成员行必须以制表符开头 → split 后 parts[0] == ""
            if prefix != "":
                continue

            if len(parts) < 2:
                continue

            member_type = parts[1]

            # 方法: m <desc> <obf> <name>
            if member_type == "m" and len(parts) >= 5:
                desc, obf_name, real_name = parts[2], parts[3], parts[4]
                self._class_methods[current_class_inter].append(
                    MethodInfo(
                        desc=desc, obf_name=obf_name, real_name=real_name, params={}
                    )
                )
                current_method_obf = obf_name
                continue

            # 字段: f <jtype> <obf> <name>
            if member_type == "f" and len(parts) >= 5:
                jtype, obf_name, real_name = parts[2], parts[3], parts[4]
                self._class_fields[current_class_inter].append(
                    FieldInfo(jtype=jtype, obf_name=obf_name, real_name=real_name)
                )
                continue

            # 参数名: p <index> <name>
            if parts[2] == "p" and len(parts) >= 5:
                try:
                    param_index = int(parts[3])
                    param_name = parts[5]
                    # 找到当前类中最后一个方法（即刚添加的那个）
                    methods = self._class_methods[current_class_inter]
                    if methods and methods[-1].obf_name == current_method_obf:
                        methods[-1].params[param_index] = param_name  # 👈 直接写入
                except (ValueError, IndexError):
                    pass

        # 构建简单类名索引
        for inter, named in self._inter_to_named.items():
            simple_inter = inter.split("/")[-1]
            simple_named = named.split("/")[-1]
            self._simple_to_inter[simple_inter].append(inter)
            self._simple_to_named[simple_named].append(named)

        MAPPER = self

    def _reverse_dict(self, dict_obj: dict) -> dict:
        """反转key-value 到 value-key"""
        _reversed_dict = {}
        for k, v in dict_obj.items():
            _reversed_dict[v] = k
        return _reversed_dict

    def obf_class(self, readable_class: str) -> str:
        """可读类名→ 混淆类名"""
        named_key = self.deobf_class(readable_class)
        obf = self._named_to_inter.get(named_key.replace(".", "/"), readable_class)
        return obf.replace("/", ".")

    def deobf_class(self, class_hint: str) -> str:
        """
        智能反混淆类名。
        支持：
        - 完整混淆名（点号）："net.minecraft.class_746"
        - 完整可读名（点号）："net.minecraft.client.network.ClientPlayerEntity"
        - 简单混淆名："class_746"
        - 简单可读名："ClientPlayerEntity"

        如果找到多个匹配，返回第一个；如果没找到，原样返回。
        """
        # 标准化为 slash 格式（用于内部查找）
        if "." in class_hint:
            slash_hint = class_hint.replace(".", "/")
        else:
            slash_hint = class_hint  # 可能是简单名

        # 1. 先尝试作为完整 intermediary 名查找
        if slash_hint in self._inter_to_named:
            return self._inter_to_named[slash_hint].replace("/", ".")

        # 2. 尝试作为完整 named 名查找（反向映射）
        if slash_hint in self._named_to_inter:
            return slash_hint.replace("/", ".")  # 已经是可读名

        # 3. 尝试作为简单名（intermediary）
        if class_hint in self._simple_to_inter:
            matches = self._simple_to_inter[class_hint]
            if matches:
                # 返回第一个匹配的可读名
                return self._inter_to_named[matches[0]].replace("/", ".")

        # 4. 尝试作为简单名（named）
        if class_hint in self._simple_to_named:
            matches = self._simple_to_named[class_hint]
            if matches:
                return matches[0].replace("/", ".")

        # 5. 找不到，原样返回
        return class_hint

    def deobf_method(self, readable_class: str, method_name: str) -> MethodInfo | None:
        """找方法"""
        info = self.get_class_info_by_readable(readable_class)
        for m in info.methods:
            if m.real_name == method_name:
                return m
        return None

    def deobf_field(self, readable_class: str, field_name: str) -> FieldInfo | None:
        """找字段"""
        info = self.get_class_info_by_readable(readable_class)
        for f in info.fields:
            if f.real_name == field_name:
                return f
        return None

    def find_class(self, pattern: str) -> list[tuple[str, str]]:
        """
        模糊搜索可读类名中包含 keyword 的类。

        返回列表：[(intermediary_dot_format, readable_dot_format), ...]
        例如：
            mapper.find_class("Player")
            → [('net.minecraft.class_1657', 'net.minecraft.entity.player.PlayerEntity'), ...]
        """
        regex = re.compile(pattern)
        result = []
        for inter, named in self._inter_to_named.items():
            if regex.search(named):
                result.append((inter.replace("/", "."), named.replace("/", ".")))
        return result

    def find_class_with_info(self, keyword: str) -> list[MappedClassInfo]:
        matches = self.find_class(keyword)
        return [self.get_class_info(obf) for obf, _ in matches]

    def get_all_readable_classes(self) -> list[str]:
        return sorted({v.replace("/", ".") for v in self._inter_to_named.values()})

    @lru_cache(maxsize=1024)
    def get_class_info(self, obf_class: str) -> MappedClassInfo:
        """
        返回完整类信息，包含字段/方法的类型、混淆名、可读名。
        """
        inter_key = obf_class.replace(".", "/")
        readable_class = self._inter_to_named.get(inter_key, obf_class).replace(
            "/", "."
        )
        obf_class_dot = obf_class  # preserve input format

        methods = self._class_methods.get(inter_key, [])
        fields = self._class_fields.get(inter_key, [])

        return MappedClassInfo(
            class_name=readable_class,
            obf_class_name=obf_class_dot,
            methods=methods,
            fields=fields,
        )

    def get_class_info_by_readable(self, readable_class: str) -> MappedClassInfo:
        obf = self.obf_class(readable_class)
        return self.get_class_info(obf)
