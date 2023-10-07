#!/usr/bin/python3

from enum import Enum
from dataclasses import dataclass, field

from .Ranges import NumericRange

@dataclass
class BeerStyle(Jsonable) :
    # Basic characteristics
    location : str = field(default_factory=str)
    category : str = field(default_factory=str)
    
    # Descriptions
    description : str = field(default_factory=str)
    color : str  = field(default_factory=str)
    body : str = field(default_factory=str)
    malt_flavors : str = field(default_factory=str)
    hop_flavors : str = field(default_factory=str)
    ibu : str = field(default_factory=str)
    fermentation_characteristics : str = field(default_factory=str)
    commercial_examples : list[str] = field(default_factory=list)

    # Style details
    abv : NumericRange = field(default_factory=NumericRange)
    ibu : NumericRange = field(default_factory=NumericRange)
    srm : NumericRange = field(default_factory=NumericRange)
    original_gravity : NumericRange = field(default_factory=NumericRange)
    final_gravity : NumericRange = field(default_factory=NumericRange)
    



