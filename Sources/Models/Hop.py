#!/usr/bin/python3

from enum import Enum
from dataclasses import field, dataclass
from typing import cast, Any

from .Ranges import RatioRange, NumericRange
from .ScapedObject import ScrapedObject

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
class Hop(ScrapedObject) :
    # Basic characteristics
    name : str = field(default_factory=str)
    link : str = field(default_factory=str)
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

    def __eq__(self, other: object) -> bool:

        if type(self) != type(other) :
            return False
        other = cast(Hop, other)
        self = cast(Hop, self)
        identical = True
        identical &= super().__eq__(other)
        identical &= self.name ==  other.name
        identical &= self.link == other.link
        identical &= self.purpose == other.purpose
        identical &= self.country == other.country
        identical &= self.international_code == other.international_code
        identical &= self.cultivar_id == other.cultivar_id
        identical &= self.origin_txt == other.origin_txt
        identical &= self.flavor_txt == other.flavor_txt
        identical &= self.tags == other.tags
        identical &= self.alpha_acids == other.alpha_acids
        identical &= self.beta_acids == other.beta_acids
        identical &= self.alpha_beta_ratio == other.alpha_beta_ratio
        identical &= self.hop_storage_index == other.hop_storage_index
        identical &= self.co_humulone_normalized == other.co_humulone_normalized
        identical &= self.total_oils == other.total_oils
        identical &= self.myrcene == other.myrcene
        identical &= self.humulene == other.humulene
        identical &= self.caryophyllene == other.caryophyllene
        identical &= self.farnesene == other.farnesene
        identical &= self.other_oils == other.other_oils
        identical &= self.beer_styles == other.beer_styles
        identical &= self.substitutes == other.substitutes

        return identical

    def to_json(self) -> dict[str, Any] :
        content = {
            "name" : self.name,
            "link" : self.link,
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

        # Doing the parent at the end as it only contains the parsingErrors that we want to go to the end of the object
        content.update(super().to_json())
        return content

    def from_json(self, content : dict[str, Any]) -> None :
        super().from_json(content)

        self.name = self._read_prop("name", content, "")
        self.link = self._read_prop("link", content, "")
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