"""
this module contains tests that cover functions from magnit scraper
"""

import json
from decimal import Decimal
from pathlib import Path

import pytest

from scrapers.common import (Category, Product, ProductInfo, ProductList,
                             RequestData, get_today_date)
from scrapers.magnit.scraper import parse_data


# path to example catalogue product list data
EXAMPLE_PRODUCT_LIST = (
    Path(__file__).parent / 'data' / 'example_product_list_magnit.json')


@pytest.fixture
def example_product_list():
    with open(EXAMPLE_PRODUCT_LIST, 'r') as f:
        return json.load(f)


@pytest.fixture
def category():
    """
    represents a Category object
    """
    return Category(
        supermarket_id=3,
        category_id=1,
        category_code="12345",
        name="Some name",
        last_scraped_on=None
    )


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
                product_code='1000006090',
                category_id=1,
                name='Сливки Магнит стерилизованные 10%  500мл',
                url='https://magnit.ru/product/1000006090?shopCode=703059',
                created_on=today,
                product_info=ProductInfo(
                    product_id=None,
                    observed_on=today,
                    price=Decimal('259.99'),
                    discounted_price=Decimal('129.99'),
                    rating=Decimal('4.8'),
                    rates_count=1500,
                    unit=None
                )
            ),
            Product(
                product_id=None,
                product_code='1000467851',
                category_id=1,
                name='Сливки Campina порционные 10% 100г',
                url='https://magnit.ru/product/1000467851?shopCode=703059',
                created_on=today,
                product_info=ProductInfo(
                    product_id=None,
                    observed_on=today,
                    price=Decimal('54.99'),
                    discounted_price=None,
                    rating=Decimal('4.8'),
                    rates_count=1262,
                    unit=None
                )
            ),
        ]
    )
    assert parse_data(data_from_request, category) == expected_result
