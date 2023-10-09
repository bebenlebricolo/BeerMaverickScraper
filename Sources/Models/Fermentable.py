#!/usr/bin/python3

from enum import Enum
from dataclasses import dataclass, field

from .Ranges import NumericRange
from .Jsonable import *


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
    diastatic_power : float = 0
    ppg : float = 0
    batch_max : float = 0
    



