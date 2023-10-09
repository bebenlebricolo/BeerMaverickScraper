from dataclasses import dataclass
from .Jsonable import *

class NumericRange(Jsonable):
    min : JsonProperty[float] = field(default_factory=JsonProperty[float])
    max : JsonProperty[float] = field(default_factory=JsonProperty[float])

    def __init__(self, min : float = 0, max : float = 0) -> None:
        super().__init__()
        self.min = JsonProperty[float]("min", min)
        self.max = JsonProperty[float]("max", max)

    def to_json(self) -> dict:
        return {
            self.min._prop_key: self.min.value,
            self.max._prop_key: self.max.value
        }

    def from_json(self, content: dict) -> None:
        self.min.value = float(self.min.try_read(content, self.min.value))
        self.max.value = float(self.max.try_read(content, self.max.value))

class RatioRange(Jsonable):
    min : JsonProperty[str] = field(default_factory=JsonProperty[str])
    max : JsonProperty[str] = field(default_factory=JsonProperty[str])

    def __init__(self, min : str = "", max : str = "") -> None:
        super().__init__()
        self.min = JsonProperty[str]("min", min)
        self.max = JsonProperty[str]("max", max)

    def to_json(self) -> dict:
        return {
            self.min._prop_key: self.min.value,
            self.max._prop_key: self.max.value
        }
    
    def from_json(self, content: dict) -> None:
        self.min.value = self.min.try_read(content, self.min.value)
        self.max.value = self.max.try_read(content, self.max.value)