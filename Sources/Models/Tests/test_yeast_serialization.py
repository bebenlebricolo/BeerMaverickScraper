import unittest
import json
from ..Yeast import Yeast, Packaging, Flocculation
from ..Ranges import NumericRange

class TestYeastModelSerialization(unittest.TestCase):
    def test_yeast_symmetric_json(self):
        yeast = Yeast()
        yeast.name = "Test yeast"
        yeast.link = "https://beermaverick.com/yeast/apollo/"
        yeast.brand = "some brand"
        yeast.type = "Yeast"
        yeast.packaging = Packaging.Dry
        yeast.has_bacterias = False
        yeast.species = ["Saccharomyces cerevisiae"]
        yeast.description = "A very good yeast"
        yeast.tags = ["Tag1", "Tag2", "Tag3"]
        yeast.alcohol_tolerance = 12
        yeast.attenuation = NumericRange(70,80)
        yeast.flocculation = Flocculation.High
        yeast.optimal_temperature = NumericRange(16,23)
        yeast.comparable_yeasts = ["yeast1", "yeast2"]
        yeast.common_beer_styles = ["Pale Ale", "Funky style"]


        content = yeast.to_json()
        print(json.dumps(content, indent=4))
        parsed_object = Yeast()
        parsed_object.from_json(content)

        # Data is good but we need a custom comparator
        self.assertEqual(yeast, parsed_object)

if __name__ == "__main__" :
    unittest.main()