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
    other_oils : NumericRange = field(default_factory=NumericRange)

    # Textual description of beer styles, this is written in length text.
    # Assuming the populating data comes from an Api of some sort, we can probably identify a pattern and remove it programmatically to 
    # just retrieve the styles themselves.
    beer_styles : list[str] = field(default_factory=list)
    
    # May be changed to use a unique id instead of "just" the hop name, using the pointed URL and maybe a map that pairs an URL to a unique hop
    # For now this will do, especially if there is no name conflict.
    substitutes : list[str] = field(default_factory=list)

    def to_json(self) -> dict :
        return {
            "name" : self.name,
            "origLink" : self.orig_link,
            "purpose" : self.purpose.value,
            "country" : self.country,
            "internationalCode" : self.international_code,
            "cultivarId" : self.cultivar_id,
            "originTxt" : self.origin_txt,
            "flavorTxt" : self.flavor_txt,
            "tags" : self.tags,
            "alphaAcids" : self.alpha_acids.to_json(),
            "betaAcids" : self.beta_acids.to_json(),
            "alphaBetaRatio" : self.alpha_beta_ratio.to_json(),
            "hopStorageIndex" : self.hop_storage_index,
            "coHumuloneNormalized" : self.co_humulone_normalized.to_json(),
            "totalOils" : self.total_oils.to_json(),
            "myrcene" : self.myrcene.to_json(),
            "humulene" : self.humulene.to_json(),
            "caryophyllene"  : self.caryophyllene.to_json(),
            "farnesene" : self.farnesene.to_json(),
            "otherOils" : self.other_oils.to_json(),
            "beerStyles" : self.beer_styles,
            "substitutes" : self.substitutes
        }
    
    def from_json(self, content : dict) -> None :
        self.name = self._read_prop("name", content, "")
        self.orig_link = self._read_prop("origLink", content, "")
        self.purpose = hop_attribute_from_str(self._read_prop("purpose", content, ""))
        self.country = self._read_prop("country", content, "")
        self.international_code = self._read_prop("internationalCode", content, "")
        self.cultivar_id = self._read_prop("cultivarId", content, "")
        self.origin_txt = self._read_prop("originTxt", content, "")
        self.flavor_txt = self._read_prop("flavorTxt", content, "")
        self.tags = self._read_prop("tags", content, "")
        self.alpha_acids.from_json(content["alphaAcids"])
        self.beta_acids.from_json(content["betaAcids"])
        self.alpha_beta_ratio.from_json(content["alphaBetaRatio"])
        self.hop_storage_index = self._read_prop("hopStorageIndex", content, 80)
        self.co_humulone_normalized.from_json(content["coHumuloneNormalized"])
        self.total_oils.from_json(content["totalOils"])
        self.myrcene.from_json(content["myrcene"])
        self.humulene.from_json(content["humulene"])
        self.caryophyllene.from_json(content["caryophyllene"])
        self.farnesene.from_json(content["farnesene"])
        self.other_oils.from_json(content["otherOils"])
        self.beer_styles = self._read_prop("beerStyles", content, [])
        self.substitutes = self._read_prop("substitutes", content, [])

