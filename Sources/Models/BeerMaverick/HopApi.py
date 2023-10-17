from dataclasses import dataclass, field
from typing import Any, Optional
from ..Jsonable import Jsonable


@dataclass
class BMHopOils(Jsonable):
    myr : list[str] = field(default_factory=list[str])
    hum : list[str] = field(default_factory=list[str])
    car : list[str] = field(default_factory=list[str])
    far : list[str] = field(default_factory=list[str])

    # This is raw dumping, I don't care about having a clean model as I don't plan to remap the keys or
    # anything else.
    def to_json(self) -> dict[str, Any]:
        return self.__dict__

    def from_json(self, content: dict[str, Any]) -> None:
        self.__dict__ = content

@dataclass
class BMPrimaryHopModel(Jsonable):
    slug : str                  = field(default_factory=str)
    name : str                  = field(default_factory=str)
    country : str               = field(default_factory=str)
    purpose : str               = field(default_factory=str)
    alpha : list[str]           = field(default_factory=list[str])
    beta : list[str]            = field(default_factory=list[str])
    cohumulone : list[str]      = field(default_factory=list[str])
    total_oils : list[str]      = field(default_factory=list[str])
    oils : BMHopOils            = field(default_factory=BMHopOils)
    beer_styles : list[str]     = field(default_factory=list[str])
    characteristics : str       = field(default_factory=str)
    heritage : str              = field(default_factory=str)
    radar_chart : list[int]     = field(default_factory=list[int])
    used_with : list[str]       = field(default_factory=list[str])
    cryo : str                  = field(default_factory=str)
    tags : list[str]            = field(default_factory=list[str])
    readmore : list[str]        = field(default_factory=list[str])
    beer_examples : list[str]   = field(default_factory=list[str])
    owned_by : Optional[str]    = field(default=None)
    statistics : list[str]      = field(default_factory=list[str])
    official_id : list[str]     = field(default_factory=list[str])
    image : Optional[str]       = field(default=None)
    active : int                = field(default=1)
    hsi : Optional[str]         = field(default=None)
    yvh : str                   = field(default_factory=str)
    country_name : str          = field(default_factory=str)

    # This is raw dumping, I don't care about having a clean model as I don't plan to remap the keys or
    # anything else.
    def to_json(self) -> dict[str, Any]:
        return self.__dict__

    def from_json(self, content: dict[str, Any]) -> None:
        self.__dict__ = content

@dataclass
class BMHopCorrelationElem(Jsonable):
    name : str = field(default_factory=str)
    slug : str = field(default_factory=str)
    correlation : float = field(default=0.0)

@dataclass
class BMHopSubstitute(Jsonable):
    human_picked : list[str]            = field(default_factory=list[str])
    aroma : list[BMHopCorrelationElem]  = field(default_factory=list[BMHopCorrelationElem])
    combined : list[BMHopCorrelationElem]  = field(default_factory=list[BMHopCorrelationElem])
    bittering : list[BMHopCorrelationElem]  = field(default_factory=list[BMHopCorrelationElem])

    # This is raw dumping, I don't care about having a clean model as I don't plan to remap the keys or
    # anything else.
    def to_json(self) -> dict[str, Any]:
        return self.__dict__

    def from_json(self, content: dict[str, Any]) -> None:
        self.__dict__ = content

@dataclass
class BMHopModel(Jsonable) :
    substitute : BMHopSubstitute = field(default_factory=BMHopSubstitute)
    primary : BMPrimaryHopModel  = field(default_factory=BMPrimaryHopModel)


    # This is raw dumping, I don't care about having a clean model as I don't plan to remap the keys or
    # anything else.
    def to_json(self) -> dict[str, Any]:
        return self.__dict__

    def from_json(self, content: dict[str, Any]) -> None:
        self.primary.from_json(content["primary"])
        self.substitute.from_json(content["substitute"])