from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum


from .Ranges import NumericRange
from .Jsonable import *
from .ScapedObject import ScrapedObject

class Flocculation(Enum) :
    VeryLow = "Very Low"
    Low = "Low"
    LowMedium = "Low-Medium"
    Medium = "Medium"
    MediumHigh = "Medium-High"
    High = "High"
    VeryHigh = "Very High"
    Unknown = "Unknown"

def str_to_flocculation(input : Optional[str]) -> Flocculation:
    if input == None :
        return Flocculation.Unknown

    data = [
        Flocculation.VeryLow,
        Flocculation.Low,
        Flocculation.LowMedium,
        Flocculation.Medium,
        Flocculation.MediumHigh,
        Flocculation.High,
        Flocculation.VeryHigh,
    ]
    for item in data :
        if input.lower() == item.value.lower() :
            return item

    # Some yeast has the "Medium-Low" tag, which is more or less "Low-Medium" in reverse (...).
    # Dedicated corner case parsing here :)
    if Flocculation.Medium.value.lower() in input.lower() and Flocculation.Low.value.lower() in input.lower():
        return Flocculation.LowMedium
    return Flocculation.Unknown

class Packaging(Enum) :
    Liquid = "Liquid"
    Dry = "Dry"
    Unknown = "Unknown"

def str_to_packaging(input : Optional[str]) -> Packaging:
    if input == None :
        return Packaging.Unknown

    data = [
        Packaging.Liquid,
        Packaging.Dry
    ]

    for item in data :
        if item.value.lower() == input.lower():
            return item
    return Packaging.Unknown

@dataclass
class Yeast(ScrapedObject):
    name :                  str                     = field(default_factory=str)
    brand :                 str                     = field(default_factory=str)
    link :                  str                     = field(default_factory=str)
    type :                  str                     = field(default_factory=str)
    packaging :             Packaging               = field(default=Packaging.Unknown)
    has_bacterias :         bool                    = field(default=False)
    species :               list[str]               = field(default_factory=list)

    description :           str                     = field(default_factory=str)
    tags :                  list[str]               = field(default_factory=list)
    alcohol_tolerance :     Optional[float]         = field(default=None)
    attenuation :           Optional[NumericRange]  = field(default=None)
    flocculation :          Flocculation            = field(default=Flocculation.Unknown)
    optimal_temperature :   Optional[NumericRange]  = field(default=None)

    comparable_yeasts :     list[str]               = field(default_factory=list)
    common_beer_styles :    list[str]               = field(default_factory=list)

    def from_json(self, content: dict[str, Any]) -> None:
        self.name = self._read_prop("name", content, "")
        self.id = self._read_prop("id", content, "")
        self.brand = self._read_prop("brand", content, "")
        self.link = self._read_prop("link", content, "")
        self.type = self._read_prop("type", content, "")

        self.packaging = str_to_packaging(content["packaging"])
        self.has_bacterias = bool(self._read_prop("hasBacterias", content, False))
        self.species = []
        for item in content["species"] :
            self.species.append(item)

        self.description = self._read_prop("description", content, "")
        self.tags = []
        for item in content["tags"] :
            self.tags.append(item)

        if "alcoholTolerance" in content and content["alcoholTolerance"]:
            self.alcohol_tolerance = float(content["alcoholTolerance"])
        self.attenuation = self.parse_numeric_range("attenuation", content)
        self.flocculation = str_to_flocculation(content["flocculation"])

        self.optimal_temperature = self.parse_numeric_range("optimalTemperature", content)
        self.comparable_yeasts = []
        for item in content["comparableYeasts"] :
            self.comparable_yeasts.append(item)

        self.common_beer_styles = []
        for item in content["commonBeerStyles"] :
            self.common_beer_styles.append(item)

        self.parsing_errors = self._read_prop("parsingErrors", content, None)


    def to_json(self) -> dict[str, Any]:
        content = {
            "name" : self.name,
            "id" : self.id,
            "brand" : self.brand,
            "link" : self.link,
            "type" : self.type,
            "packaging" : self.packaging._value_,
            "hasBacterias" : self.has_bacterias,
            "species" : self.species,
            "description" : self.description,
            "tags" : self.tags,
            "alcoholTolerance" : self.alcohol_tolerance,
            "attenuation" : self.attenuation.to_json() if self.attenuation else None,
            "flocculation" : self.flocculation.value,
            "optimalTemperature" : self.optimal_temperature.to_json() if self.optimal_temperature else None,
            "comparableYeasts" : self.comparable_yeasts,
            "commonBeerStyles" : self.common_beer_styles
        }
        content.update(super().to_json())
        return content

    def __eq__(self, other: object) -> bool:
        if type(self) != type(other) :
            return False

        identical = super().__eq__(other)
        other = cast(Yeast, other)
        self = cast(Yeast, self)
        identical &= self.name == other.name
        identical &= self.id == other.id
        identical &= self.brand == other.brand
        identical &= self.link == other.link
        identical &= self.type == other.type
        identical &= self.packaging == other.packaging
        identical &= self.has_bacterias == other.has_bacterias
        identical &= self.species == other.species
        identical &= self.description == other.description
        identical &= self.tags == other.tags
        identical &= self.alcohol_tolerance == other.alcohol_tolerance
        identical &= self.attenuation == other.attenuation
        identical &= self.flocculation == other.flocculation
        identical &= self.optimal_temperature == other.optimal_temperature
        identical &= self.comparable_yeasts == other.comparable_yeasts
        identical &= self.common_beer_styles == other.common_beer_styles
        return identical

