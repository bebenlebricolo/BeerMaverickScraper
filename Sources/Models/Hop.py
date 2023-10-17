#!/usr/bin/python3

from enum import Enum
from dataclasses import field, dataclass
from typing import cast, Any

from .Ranges import RatioRange, NumericRange
from .ScapedObject import ScrapedObject
from .Jsonable import Jsonable
from .BeerMaverick import HopApi as bmapi

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
class RadarChart(Jsonable) :
    citrus : int            = field(default=0)
    tropical_fruit : int    = field(default=0)
    stone_fruit : int       = field(default=0)
    berry : int             = field(default=0)
    floral : int            = field(default=0)
    grassy : int            = field(default=0)
    herbal : int            = field(default=0)
    spice : int             = field(default=0)
    resinous : int          = field(default=0)

    def to_json(self) -> dict[str, Any]:
        return {
            "citrus" : self.citrus,
            "tropicalFruit" : self.tropical_fruit,
            "stoneFruit" : self.stone_fruit,
            "berry" : self.berry,
            "floral" : self.floral,
            "grassy" : self.grassy,
            "herbal" : self.herbal,
            "spice" : self.spice,
            "resinous" : self.resinous
        }

    def from_json(self, content: dict[str, Any]) -> None:
        self.citrus = self._read_prop("citrus", content, 0)
        self.tropical_fruit = self._read_prop("tropicalFruit", content, 0)
        self.stone_fruit = self._read_prop("stoneFruit", content, 0)
        self.berry = self._read_prop("berry", content, 0)
        self.floral = self._read_prop("floral", content, 0)
        self.grassy = self._read_prop("grassy", content, 0)
        self.herbal = self._read_prop("herbal", content, 0)
        self.spice = self._read_prop("spice", content, 0)
        self.resinous = self._read_prop("resinous", content, 0)


@dataclass
class Hop(ScrapedObject) :
    # Basic characteristics
    name : str                  = field(default_factory=str)
    link : str                  = field(default_factory=str)
    purpose : HopAttribute      = HopAttribute.Bittering
    country : str               = field(default_factory=str)
    international_code : str    = field(default_factory=str)
    cultivar_id : str           = field(default_factory=str)
    origin_txt : str            = field(default_factory=str)
    flavor_txt : str            = field(default_factory=str)
    tags : list[str]            = field(default_factory=list)

    # Hop detailed characteristics
    alpha_acids : NumericRange              = field(default_factory=NumericRange)
    beta_acids : NumericRange               = field(default_factory=NumericRange)
    alpha_beta_ratio : RatioRange           = field(default_factory=RatioRange)
    hop_storage_index : float               = 100
    co_humulone_normalized : NumericRange   = field(default_factory=NumericRange)
    total_oils : NumericRange               = field(default_factory=NumericRange)

    # Oil contents
    myrcene : NumericRange          = field(default_factory=NumericRange)
    humulene : NumericRange         = field(default_factory=NumericRange)
    caryophyllene : NumericRange    = field(default_factory=NumericRange)
    farnesene : NumericRange        = field(default_factory=NumericRange)
    other_oils : NumericRange       = field(default_factory=NumericRange)

    # Textual description of beer styles, this is written in length text.
    # Assuming the populating data comes from an Api of some sort, we can probably identify a pattern and remove it programmatically to
    # just retrieve the styles themselves.
    beer_styles : list[str] = field(default_factory=list)

    # May be changed to use a unique id instead of "just" the hop name, using the pointed URL and maybe a map that pairs an URL to a unique hop
    # For now this will do, especially if there is no name conflict.
    substitutes : list[str] = field(default_factory=list)

    radar_chart : RadarChart = field(default_factory=RadarChart)

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
        identical &= self.radar_chart == other.radar_chart

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
            "substitutes" : self.substitutes,
            "radarChart" : self.radar_chart.to_json()
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

        self.radar_chart.from_json(content["radarChart"])

    def radar_chart_from_bmapi(self, api_model : bmapi.BMHopModel) :
        self.radar_chart.citrus = api_model.primary.radar_chart[0]
        self.radar_chart.tropical_fruit = api_model.primary.radar_chart[1]
        self.radar_chart.stone_fruit = api_model.primary.radar_chart[2]
        self.radar_chart.berry = api_model.primary.radar_chart[3]
        self.radar_chart.floral = api_model.primary.radar_chart[4]
        self.radar_chart.grassy = api_model.primary.radar_chart[5]
        self.radar_chart.herbal = api_model.primary.radar_chart[6]
        self.radar_chart.spice = api_model.primary.radar_chart[7]
        self.radar_chart.resinous = api_model.primary.radar_chart[8]


        # Some text