import unittest
import json
from ..Hop import Hop, HopAttribute

class TestHopModelSerialization(unittest.TestCase):
    def test_hop_symmetric_json(self):
        hop = Hop()
        hop.name = "Test hop"
        hop.link = "https://beermaverick.com/hop/apollo/"
        hop.purpose = HopAttribute.Aromatic
        hop.country = "don't know"
        hop.international_code = "maybe"
        hop.cultivar_id = "perhaps"
        hop.origin_txt = "Some origin"
        hop.flavor_txt = "some flavor"
        hop.tags = ["tag1", "tag2", "tag3"]

        hop.alpha_acids.max.value = 3
        hop.alpha_acids.min.value = 2

        hop.beta_acids.max.value = 4
        hop.beta_acids.min.value = 5

        hop.alpha_beta_ratio.max.value = "3:2"
        hop.alpha_beta_ratio.min.value = "2:1"

        hop.hop_storage_index = 85
        hop.co_humulone_normalized.max.value = 66
        hop.co_humulone_normalized.min.value = 99

        hop.total_oils.max.value = 1
        hop.total_oils.min.value = 2

        hop.myrcene.max.value = 3
        hop.myrcene.min.value = 4

        hop.humulene.max.value = 5
        hop.humulene.min.value = 6

        hop.caryophyllene.max.value = 7
        hop.caryophyllene.min.value = 8

        hop.farnesene.max.value = 9
        hop.farnesene.min.value = 10

        hop.other_oils.max.value = 11
        hop.other_oils.min.value = 12

        hop.beer_styles = ["Ale", "Sour", "Don't know"]
        hop.substitutes = ["Substitute 1", "Substitute 2", "Substitute 3"]

        content = hop.to_json()
        print(json.dumps(content, indent=4))
        parsed_hop = Hop()
        parsed_hop.from_json(content)

        # Data is good but we need a custom comparator
        self.assertEqual(hop, parsed_hop)

if __name__ == "__main__" :
    unittest.main()