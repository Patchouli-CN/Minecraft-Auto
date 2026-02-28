from datetime import timedelta
from typing import TypedDict


class RegexRule(TypedDict):
    """具体规则"""

    regex: str
    """ 正则表达式 """
    groups: list[str]
    """ 要匹配的字段 """


class EventRegexRules(TypedDict):
    """事件规则字典"""

    name: str
    """ 事件名字 """
    rules: list[RegexRule]


type TimeDeltaLike = timedelta | int | float
""" 看起来像是时间间隔 """
