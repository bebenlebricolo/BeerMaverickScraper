#!/usr/bin/python3

from enum import Enum
from dataclasses import dataclass, field
from typing import Optional

from .Ranges import NumericRange
from .Jsonable import *


@dataclass
class Water(Jsonable) :
    # Basic characteristics
    water_impact : str = field(default_factory=str)
    scientific_name : Optional[str] = field(default_factory=str)
    chemical_element : Optional[str] = None
    
    # Descriptions
    uses : str = field(default_factory=str)
    dosage : Optional[str] = field(default_factory=str)
