"""
this module contains tests that cover functions from pyaterochka scraper
"""

import json
from pathlib import Path

import pytest

from scrapers.common import Category
from scrapers.pyaterochka.catalogue import parse_categories


# path to example catalogue data
EXAMPLE_CATALOGUE = Path(__file__).parent / 'example_catalogue_pyaterochka.json'


@pytest.fixture
def example_data():
    with open(EXAMPLE_CATALOGUE, 'r') as f:
        return json.load(f)


def test_parse_categories(example_data):
    """
    test that correct categories were parsed from the file
    """
    expected_result = [
        Category(supermarket_id=None, category_id=None,
                 inner_code='73C20455', name='Готовим оливье'),
        Category(supermarket_id=None, category_id=None,
                 inner_code='73C20456', name='Запекаем в духовке'),
        Category(supermarket_id=None, category_id=None,
                 inner_code='73C10301', name='Горячие напитки'),
        Category(supermarket_id=None, category_id=None,
                 inner_code='73C9714', name='Блинчики, сырники и каши'),
    ]
    parsed_categories = parse_categories(example_data)
    assert expected_result == parsed_categories
