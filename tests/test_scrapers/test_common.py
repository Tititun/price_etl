"""
this module contains tests that cover functions from scrapers' common module
"""
import datetime
from decimal import Decimal

import pytest

from scrapers.common import parse_price, Product, ProductList


def test_parse_price_none():
    """test if parse_price returns None when value is None"""
    assert parse_price({'price': None}, 'price') is None


def test_parse_price_decimal():
    """test if parse_price returns Decimal when value is not None"""
    assert parse_price({'price': 12.99}, 'price') == Decimal('12.99')


@pytest.fixture
def product_list():
    return ProductList(
        items=[
            Product(product_id='product_id_1',
                    supermarket_id=1,
                    category_id='some_category',
                    name='Product 1',
                    created_on=datetime.date(2000, 1, 1)),
            Product(product_id='product_id_2',
                    supermarket_id=1,
                    category_id='some_category',
                    name='Product 2',
                    created_on=datetime.date(2000, 1, 1))
        ]
    )


def test_product_list_get_products_ids(product_list):
    """
    test that get_products_ids method of ProductList class returns a list
    of expected ids
    """
    assert product_list.get_products_ids() == ['product_id_1', 'product_id_2']
