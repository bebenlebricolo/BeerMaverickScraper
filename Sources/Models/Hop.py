#!/usr/bin/python3

from enum import Enum
from dataclasses import field, dataclass
from typing import Optional, cast, Any

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

        case HopAttribute.Aromatic.value | "Aroma":
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
    name :                  str            = field(default_factory=str)
    link :                  str            = field(default_factory=str)
    purpose :               HopAttribute   = HopAttribute.Bittering
    country :               Optional[str]  = field(default=None)
    international_code :    Optional[str]  = field(default=None)
    cultivar_id :           Optional[str]  = field(default=None)
    ownership:              Optional[str]  = field(default=None)
    origin_txt :            str            = field(default_factory=str)
    flavor_txt :            str            = field(default_factory=str)
    tags :                  list[str]      = field(default_factory=list)

    # Hop detailed characteristics
    alpha_acids :               Optional[NumericRange]  = field(default=None)
    beta_acids :                Optional[NumericRange]  = field(default=None)
    alpha_beta_ratio :          Optional[RatioRange]    = field(default=None)
    hop_storage_index :         Optional[float]         = field(default=None)
    co_humulone_normalized :    Optional[NumericRange]  = field(default=None)
    total_oils :                Optional[NumericRange]  = field(default=None)

    # Oil contents
    myrcene :       Optional[NumericRange]  = field(default=None)
    humulene :      Optional[NumericRange]  = field(default=None)
    caryophyllene : Optional[NumericRange]  = field(default=None)
    farnesene :     Optional[NumericRange]  = field(default=None)
    other_oils :    Optional[NumericRange]  = field(default=None)

    # Textual description of beer styles, this is written in length text.
    # Assuming the populating data comes from an Api of some sort, we can probably identify a pattern and remove it programmatically to
    # just retrieve the styles themselves.
    beer_styles : list[str] = field(default_factory=list)

    # May be changed to use a unique id instead of "just" the hop name, using the pointed URL and maybe a map that pairs an URL to a unique hop
    # For now this will do, especially if there is no name conflict.
    substitutes : list[str] = field(default_factory=list)

    radar_chart : Optional[RadarChart] = field(default=None)

    def __eq__(self, other: object) -> bool:
        if type(self) != type(other) :
            return False

        other = cast(Hop, other)
        self = cast(Hop, self)
        identical = True
        identical &= super().__eq__(other)
        identical &= self.id == other.id
        identical &= self.name ==  other.name
        identical &= self.link == other.link
        identical &= self.purpose == other.purpose
        identical &= self.country == other.country
        identical &= self.international_code == other.international_code
        identical &= self.ownership == other.ownership
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
            "id" : self.id,
            "link" : self.link,
            "purpose" : self.purpose.value,
            "country" : self.country,
            "internationalCode" : self.international_code,
            "ownership" : self.ownership,
            "cultivarId" : self.cultivar_id,
            "originTxt" : self.origin_txt,
            "flavorTxt" : self.flavor_txt,
            "tags" : self.tags,
            "alphaAcids" : self.alpha_acids.to_json() if self.alpha_acids else None,
            "betaAcids" : self.beta_acids.to_json() if self.beta_acids else None,
            "alphaBetaRatio" : self.alpha_beta_ratio.to_json() if self.alpha_beta_ratio else None,
            "hopStorageIndex" : self.hop_storage_index,
            "coHumuloneNormalized" : self.co_humulone_normalized.to_json() if self.co_humulone_normalized else None,
            "totalOils" : self.total_oils.to_json() if self.total_oils else None,
            "myrcene" : self.myrcene.to_json() if self.myrcene else None,
            "humulene" : self.humulene.to_json() if self.humulene else None,
            "caryophyllene"  : self.caryophyllene.to_json() if self.caryophyllene else None,
            "farnesene" : self.farnesene.to_json() if self.farnesene else None,
            "otherOils" : self.other_oils.to_json() if self.other_oils else None,
            "beerStyles" : self.beer_styles,
            "substitutes" : self.substitutes,
            "radarChart" : self.radar_chart.to_json() if self.radar_chart else None
        }

        # Doing the parent at the end as it only contains the parsingErrors that we want to go to the end of the object
        content.update(super().to_json())
        return content

    def from_json(self, content : dict[str, Any]) -> None :
        super().from_json(content)

        self.name = self._read_prop("name", content, "")
        self.id = self._read_prop("id", content, "")
        self.link = self._read_prop("link", content, "")
        self.purpose = hop_attribute_from_str(self._read_prop("purpose", content, HopAttribute.Bittering))

        # Optionals, direct read either return a value or None, so we're good with this
        self.country = content["country"]
        self.international_code = content["internationalCode"]
        self.cultivar_id = content["cultivarId"]
        self.ownership = content["ownership"]

        self.origin_txt = self._read_prop("originTxt", content, "")
        self.flavor_txt = self._read_prop("flavorTxt", content, "")
        self.tags = self._read_prop("tags", content, "")

        if "alphaAcids" in content :
            self.alpha_acids = NumericRange()
            self.alpha_acids.from_json(content["alphaAcids"])

        if "betaAcids" in content :
            self.beta_acids = NumericRange()
            self.beta_acids.from_json(content["betaAcids"])

        if "alphaBetaRatio" in content :
            self.alpha_beta_ratio = RatioRange()
            self.alpha_beta_ratio.from_json(content["alphaBetaRatio"])

        self.hop_storage_index = self._read_prop("hopStorageIndex", content, 80)

        if "coHumuloneNormalized" :
            self.co_humulone_normalized = NumericRange()
            self.co_humulone_normalized.from_json(content["coHumuloneNormalized"])

        if "totalOils" in content :
            self.total_oils = NumericRange()
            self.total_oils.from_json(content["totalOils"])

        if "myrcene" in content :
            self.myrcene = NumericRange()
            self.myrcene.from_json(content["myrcene"])

        if "humulene" in content :
            self.humulene = NumericRange()
            self.humulene.from_json(content["humulene"])

        if "caryophyllene" in content:
            self.caryophyllene = NumericRange()
            self.caryophyllene.from_json(content["caryophyllene"])

        if "farnesene" in content:
            self.farnesene = NumericRange()
            self.farnesene.from_json(content["farnesene"])

        if "otherOils" in content :
            self.other_oils = NumericRange()
            self.other_oils.from_json(content["otherOils"])
        self.beer_styles = self._read_prop("beerStyles", content, [])
        self.substitutes = self._read_prop("substitutes", content, [])

        if "radarChart" in  content :
            self.radar_chart = RadarChart()
            self.radar_chart.from_json(content["radarChart"])

    def radar_chart_from_bmapi(self, api_model : bmapi.BMHopModel) :
        """Converts the radar chart from the api into an internal model and scan for all zeros.
           In case incoming data only contains zeros, it means that data is lacking the radar chart values and we can reject it."""
        empty = True
        for item in api_model.primary.radar_chart:
            if item != 0 :
                empty = False
                break
        # Empty data model won't be usable later on
        # So check for all zeros, and if so it means we are lacking data.
        # It'll be easier for consumer code to tell it's missing data and we can't redraw the radar chart.
        if empty :
            self.add_parsing_error("No data for radar chart")
            return

        if self.radar_chart == None :
            self.radar_chart = RadarChart()
        self.radar_chart.citrus = api_model.primary.radar_chart[0]
        self.radar_chart.tropical_fruit = api_model.primary.radar_chart[1]
        self.radar_chart.stone_fruit = api_model.primary.radar_chart[2]
        self.radar_chart.berry = api_model.primary.radar_chart[3]
        self.radar_chart.floral = api_model.primary.radar_chart[4]
        self.radar_chart.grassy = api_model.primary.radar_chart[5]
        self.radar_chart.herbal = api_model.primary.radar_chart[6]
        self.radar_chart.spice = api_model.primary.radar_chart[7]
        self.radar_chart.resinous = api_model.primary.radar_chart[8]
