from dataclasses import dataclass, field
from typing import Any, Optional

from .Ranges import NumericRange
from .Jsonable import *
from .ScapedObject import ScrapedObject

@dataclass
class Yeast(ScrapedObject):
    name : str = field(default_factory=str)
    brand : str = field(default_factory=str)
    link : str = field(default_factory=str)
    type : str = field(default_factory=str)
    packaging : str = field(default_factory=str)
    has_bacterias : Optional[bool] = None
    species : list[str] = field(default_factory=list)

    description : str = field(default_factory=str)
    tags : list[str] = field(default_factory=list)
    alcohol_tolerance : float = 0
    attenuation : NumericRange = field(default_factory=NumericRange)
    flocculation : str = field(default_factory=str)
    optimal_temperature : NumericRange = field(default_factory=NumericRange)

    comparable_yeasts : list[str] = field(default_factory=list)
    common_beer_styles : list[str] = field(default_factory=list)

    # Encodes the various warnings and issues found while parsing yeast from website
    parsing_errors : Optional[list[str]] = None

    def from_json(self, content: dict[str, Any]) -> None:
        self.name = self._read_prop("name", content, "")
        self.id = self._read_prop("id", content, "")
        self.brand = self._read_prop("brand", content, "")
        self.link = self._read_prop("link", content, "")
        self.type = self._read_prop("type", content, "")
        self.packaging = self._read_prop("packaging", content, "")
        self.has_bacterias = bool(self._read_prop("hasBacterias", content, False))
        self.species = []
        for item in content["species"] :
            self.species.append(item)

        self.description = self._read_prop("description", content, "")
        self.tags = []
        for item in content["tags"] :
            self.tags.append(item)

        self.alcohol_tolerance = self._read_prop("alcoholTolerance", content, "")
        self.attenuation.from_json(content["attenuation"])
        self.flocculation = self._read_prop("flocculation", content, "")
        self.optimal_temperature.from_json(content["optimalTemperature"])

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
            "packaging" : self.packaging,
            "hasBacterias" : self.has_bacterias,
            "species" : self.species,
            "description" : self.description,
            "tags" : self.tags,
            "alcoholTolerance" : self.alcohol_tolerance,
            "attenuation" : self.attenuation.to_json(),
            "flocculation" : self.flocculation,
            "optimalTemperature" : self.optimal_temperature.to_json(),
            "comparableYeasts" : self.comparable_yeasts,
            "commonBeerStyles" : self.common_beer_styles
        }
        content.update(super().to_json())
        return content

    def __eq__(self, other: object) -> bool:
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

