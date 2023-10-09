#!/usr/bin/python3

from enum import Enum
from dataclasses import dataclass, field
from .Ranges import RatioRange, NumericRange
from .Jsonable import *

class HopAttribute(Enum) :
    Bittering = "Bittering"
    Aromatic = "Aromatic"
    Hybrid = "Hybrid"


def hop_attribute_from_str(input : str) -> HopAttribute :
    match input :
        case HopAttribute.Bittering.value :
            return HopAttribute.Bittering
        
        case HopAttribute.Aromatic.value :
            return HopAttribute.Aromatic
        
        case HopAttribute.Hybrid.value | "Dual" :
            return HopAttribute.Hybrid
    
        case _ :
            return HopAttribute.Hybrid


@dataclass
class Hop(Jsonable) :
    # Basic characteristics
    name : str = field(default_factory=str)
    orig_link : str = field(default_factory=str)
    purpose : HopAttribute = HopAttribute.Bittering
    country : str = field(default_factory=str)
    international_code : str = field(default_factory=str)
    cultivar_id : str = field(default_factory=str)
    origin_txt : str = field(default_factory=str)
    flavor_txt : str = field(default_factory=str)
    tags : list[str] = field(default_factory=list)

    # Hop detailed characteristics
    alpha_acids : NumericRange = field(default_factory=NumericRange)
    beta_acids : NumericRange = field(default_factory=NumericRange)
    alpha_beta_ratio : RatioRange = field(default_factory=RatioRange)
    hop_storage_index : float = 100
    co_humulone_normalized : NumericRange = field(default_factory=NumericRange)
    total_oils : NumericRange = field(default_factory=NumericRange)

    # Oil contents
    myrcene : NumericRange = field(default_factory=NumericRange)
    humulene : NumericRange = field(default_factory=NumericRange)
    caryophyllene : NumericRange = field(default_factory=NumericRange)
    farnesene : NumericRange = field(default_factory=NumericRange)
    other : NumericRange = field(default_factory=NumericRange)

    # Textual description of beer styles, this is written in length text.
    # Assuming the populating data comes from an Api of some sort, we can probably identify a pattern and remove it programmatically to 
    # just retrieve the styles themselves.
    beer_styles : list[str] = field(default_factory=list)
    
    # May be changed to use a unique id instead of "just" the hop name, using the pointed URL and maybe a map that pairs an URL to a unique hop
    # For now this will do, especially if there is no name conflict.
    substitutes : list[str] = field(default_factory=list)



