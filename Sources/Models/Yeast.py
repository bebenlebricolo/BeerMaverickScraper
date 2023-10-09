from dataclasses import dataclass, field
from typing import Optional

from .Ranges import NumericRange
from .Jsonable import *


@dataclass
class Yeast(Jsonable):
    brand : str = field(default_factory=str)
    type : str = field(default_factory=str)
    packaging : str = field(default_factory=str)
    has_bacterias : Optional[bool] = None
    species : list[str] = field(default_factory=str)

    description : str = field(default_factory=str)
    tags : list[str] = field(default_factory=list)
    alcohol_tolerance : float = 0
    attenuation : NumericRange = field(default_factory=NumericRange)
    floculation : str = field(default_factory=list)
    optimal_temperature : NumericRange = field(default_factory=NumericRange)

    comparable_yeasts : list[str] = field(default_factory=list)
    common_beer_styles : list[str] = field(default_factory=list)

    def from_json(self, input : dict) :
        self.name = input["name"]
        self.brand = input["brand"]
        self.link = input["link"]
        self.abv_tol = input["abv_tol"]
        self.floculation = input["floculation"]
        self.attenuation_range = input["attenuation_range"]
        self.price = input["price"]
        self.temp_range = input["temp_range"]
        self.description = input["description"]


