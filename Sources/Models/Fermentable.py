#!/usr/bin/python3

from enum import Enum
from dataclasses import dataclass, field

from .Ranges import NumericRange
from .Jsonable import Jsonable

@dataclass
class Fermentable(Jsonable) :
    # Basic characteristics
    type : str = field(default_factory=str)
    species : str = field(default_factory=str)
    category : str = field(default_factory=str)
    
    # Descriptions
    description : str = field(default_factory=str)
    beer_styles : list[str] = field(default_factory=list)
    commercial_examples : list[str] = field(default_factory=list)

    # Fermentable details
    srm : NumericRange = field(default_factory=NumericRange)
    diastatic_power : float
    ppg : float
    batch_max : float
    



