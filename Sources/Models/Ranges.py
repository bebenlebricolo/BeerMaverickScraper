from dataclasses import dataclass
from .Jsonable import *

@dataclass
class NumericRange(Jsonable):
    min : float = 0
    max : float = 0

@dataclass
class RatioRange(Jsonable):
    min : str = 0
    max : str = 0
