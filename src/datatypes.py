"""
Application datatypes
"""
from collections import namedtuple
from typing import Type, Union, Literal

Num = Union[int, float]
NumCast = Union[Type[int], Type[float]]
BQResourceType = Literal["project", "dataset", "table"]

InputOutput = namedtuple('InputOutput', ['in_', 'out'])

ColHeader = Union[str, int]
