from dataclasses import dataclass
from .Jsonable import *

@dataclass
class NumericRange(Jsonable):
    min : float
    max : float

@dataclass
class RatioRange(Jsonable):
    min : str
    max : str
