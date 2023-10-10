from .Jsonable import *

class NumericRange(Jsonable):
    min : JsonProperty[float] = field(default_factory=JsonProperty[float])
    max : JsonProperty[float] = field(default_factory=JsonProperty[float])

    def __init__(self, min : float = 0, max : float = 0) -> None:
        super().__init__()
        self.min = JsonProperty[float]("min", min)
        self.max = JsonProperty[float]("max", max)

    def to_json(self) -> dict[str, Any]:
        return {
            self.min.key: self.min.value,
            self.max.key: self.max.value
        }

    def from_json(self, content: dict[str, float]) -> None:
        self.min.value = float(self.min.try_read(content, self.min.value))
        self.max.value = float(self.max.try_read(content, self.max.value))

    def __eq__(self, other: object) -> bool:
        if type(self) != type(other):
            return False
        self = cast(NumericRange, self)
        other = cast(NumericRange, other)
        identical = True
        identical &= self.max.value == other.max.value
        identical &= self.min.value == other.min.value
        return identical



class RatioRange(Jsonable):
    min : JsonProperty[str] = field(default_factory=JsonProperty[str])
    max : JsonProperty[str] = field(default_factory=JsonProperty[str])

    def __init__(self, min : str = "", max : str = "") -> None:
        super().__init__()
        self.min = JsonProperty[str]("min", min)
        self.max = JsonProperty[str]("max", max)

    def to_json(self) -> dict[str, str]:
        return {
            self.min.key: self.min.value,
            self.max.key: self.max.value
        }

    def from_json(self, content: dict[str, str]) -> None:
        self.min.value = self.min.try_read(content, self.min.value)
        self.max.value = self.max.try_read(content, self.max.value)

    def __eq__(self, other: object) -> bool:
        if type(self) != type(other):
            return False
        self = cast(RatioRange, self)
        other = cast(RatioRange, other)
        identical = True
        identical &= self.max.value == other.max.value
        identical &= self.min.value == other.min.value
        return identical