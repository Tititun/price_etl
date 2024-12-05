"""
this module contains tests that cover functions from pyaterochka scraper
"""

import json
from pathlib import Path

from scrapers.common import Category
from scrapers.pyaterochka.catalogue import parse_categories


# path to example catalogue data
EXAMPLE_CATALOGUE = Path(__file__).parent / 'example_catalogue_pyaterochka.json'


def test_parse_categories_count():
    """
    test that parse_categories parses only the categorie from the second
     (lowest) level
    """
    with open(EXAMPLE_CATALOGUE, 'r') as f:
        example_data = json.load(f)
    data = parse_categories(example_data)
    assert len(data) == 4
