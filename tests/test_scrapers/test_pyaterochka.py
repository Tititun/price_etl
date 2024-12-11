"""
this module contains tests that cover functions from pyaterochka scraper
"""

import json
from decimal import Decimal
from pathlib import Path

import pytest
import requests

from scrapers.common import (Category, Product, ProductInfo, ProductList,
                             RequestData, get_today_date)
from scrapers.pyaterochka.catalogue import parse_categories
from scrapers.pyaterochka.scraper import parse_data, request_data


# path to example catalogue and product list data
EXAMPLE_CATALOGUE = Path(__file__).parent / 'example_catalogue_pyaterochka.json'
EXAMPLE_PRODUCT_LIST = (
        Path(__file__).parent / 'example_product_list_pyaterochka.json')


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
        Category(supermarket_id=None, category_id='73C20455',
                 name='Готовим оливье'),
        Category(supermarket_id=None, category_id='73C20456',
                 name='Запекаем в духовке'),
        Category(supermarket_id=None, category_id='73C10301',
                 name='Горячие напитки'),
        Category(supermarket_id=None, category_id='73C9714',
                 name='Блинчики, сырники и каши'),
    ]
    parsed_categories = parse_categories(example_catalogue)
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
        category_id="someID",
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

    monkeypatch.setattr(requests, 'get', mock_response)

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
                product_id='4133363',
                supermarket_id=1,
                category_id='someID',
                name='Шоколадный батончик Snickers Super 80г',
                created_on=today,
                product_info=ProductInfo(
                    product_id='4133363',
                    supermarket_id=1,
                    observed_on=today,
                    price=Decimal("69.99"),
                    discounted_price=None,
                    rating=4.94,
                    rates_count=47688,
                    unit='80 г'
                )
            ),
            Product(
                product_id='2138420',
                supermarket_id=1,
                category_id='someID',
                name='Шоколадный батончик Twix Xtra с карамелью 82г',
                created_on=today,
                product_info=ProductInfo(
                    product_id='2138420',
                    supermarket_id=1,
                    observed_on=today,
                    price=Decimal("67.99"),
                    discounted_price=Decimal("59.99"),
                    rating=4.94,
                    rates_count=28393,
                    unit='82 г'
                )
            ),
        ]
    )
    assert parse_data(data_from_request) == expected_result
