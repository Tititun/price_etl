"""
this module contains tests that cover functions from lenta scraper
"""

import json
from decimal import Decimal
from pathlib import Path

import pytest
import requests

from scrapers.common import (Category, Product, ProductInfo, ProductList,
                             RequestData, Supermarket, get_today_date)
from scrapers.lenta.catalogue import parse_categories
from scrapers.lenta.scraper import parse_data, request_data
from scrapers.lenta.common import SUPERMARKET_NAME


# path to example catalogue and product list data
EXAMPLE_CATALOGUE = (
    Path(__file__).parent / 'data' / 'example_catalogue_lenta.json')
EXAMPLE_PRODUCT_LIST = (
    Path(__file__).parent / 'data' / 'example_product_list_lenta.json')


@pytest.fixture
def example_catalogue():
    with open(EXAMPLE_CATALOGUE, 'r') as f:
        return json.load(f)


@pytest.fixture
def example_product_list():
    with open(EXAMPLE_PRODUCT_LIST, 'r') as f:
        return json.load(f)


def test_parse_categories(example_catalogue):
    """
    test that correct categories were parsed from the file
    """
    expected_result = [
        Category(supermarket_id=1, category_id=None,
                 category_code='763',
                 name='Колбаса, сосиски / Ветчина'),
        Category(supermarket_id=1, category_id=None,
                 category_code='1791',
                 name='Бытовая техника / Кухонные машины и мясорубки'),
    ]
    supermarket=Supermarket(supermarket_id=1, name=SUPERMARKET_NAME)
    parsed_categories = parse_categories(example_catalogue, supermarket)
    assert expected_result == parsed_categories


class MockResponse:
    def __init__(self, status_code):
        if 200 <= status_code < 400:
            self.ok = True
        else:
            self.ok = False
        self.status_code = status_code

    @staticmethod
    def json():
        return {'products': []}


@pytest.fixture
def category():
    """
    represents a Category object
    """
    return Category(
        supermarket_id=1,
        category_id=1,
        category_code="12345",
        name="Some name",
        last_scraped_on=None
    )


def test_request_data(category, monkeypatch):
    """
    test that request_data returns a list when response is successful
    """
    def mock_response(*args, **kwargs):
        return MockResponse(200)

    monkeypatch.setattr(requests, 'get', mock_response)
    expected_result = RequestData(category=category,
                                  data={"products": []},
                                  date=get_today_date())
    assert request_data(category) == expected_result


def test_request_data_fails(category, monkeypatch):
    """
    test that request_date returns None if request is not successful
    """
    def mock_response(*args, **kwargs):
        return MockResponse(403)

    monkeypatch.setattr(requests, 'post', mock_response)

    assert request_data(category) is None


def test_parse_data(category, example_product_list):
    """
    test if parse data  correctly parses data provided to it
    """
    today = get_today_date()
    data_from_request = RequestData(
        category=category,
        data={"products": example_product_list},
        date=today
    )
    expected_result = ProductList(
        items=[
            Product(
                product_id=None,
                product_code='54761',
                category_id=1,
                name='Мармелад жевательный БОН ПАРИ Забавный медвежонок, 75г',
                url='https://lenta.com/product/marmelad-zhevatelnyjj-bon-pari-medvedi-rossiya-75g-54761',
                created_on=today,
                product_info=ProductInfo(
                    product_id=None,
                    observed_on=today,
                    price=Decimal('73.69'),
                    discounted_price=Decimal('54.99'),
                    rating=Decimal('4.8'),
                    rates_count=12,
                    unit='75г'
                )
            ),
            Product(
                product_id=None,
                product_code='55437',
                category_id=1,
                name='Зефир ШАРМЭЛЬ с ароматом ванили, 255г',
                url='https://lenta.com/product/zefir-sharmel-s-aromatom-vanili-rossiya-255g-55437',
                created_on=today,
                product_info=ProductInfo(
                    product_id=None,
                    observed_on=today,
                    price=Decimal('168.49'),
                    discounted_price=Decimal('149.99'),
                    rating=Decimal('4.8'),
                    rates_count=78,
                    unit='255г'
                )
            ),
        ]
    )
    category = Category(supermarket_id=1, category_id=1,
                        category_code='12345', name='test_name')
    assert parse_data(data_from_request, category) == expected_result
