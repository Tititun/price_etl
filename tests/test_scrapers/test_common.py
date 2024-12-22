"""
this module contains tests that cover functions from scrapers' common module
"""
import datetime
from decimal import Decimal

import pytest

from scrapers.common import parse_price, Product, ProductInfo, ProductList


def test_parse_price_none():
    """test if parse_price returns None when value is None"""
    assert parse_price({'price': None}, 'price') is None


def test_parse_price_decimal():
    """test if parse_price returns Decimal when value is not None"""
    assert parse_price({'price': 12.99}, 'price') == Decimal('12.99')


def test_parse_price_converts():
    """
    test if parse_price converts kopecks to roubles if the unit argument
    is 'k'
    """
    result = parse_price({'price': 15099}, 'price', unit='k')
    assert result == Decimal('150.99')


def make_products(without_ids: bool=False):
    product_id_1 = 1 if not without_ids else None
    product_id_2 = 2 if not without_ids else None
    return ProductList(
        items=[
            Product(product_id=product_id_1,
                    product_code='product_id_1',
                    category_id=1,
                    name='Product 1',
                    url='url_1',
                    created_on=datetime.date(2000, 1, 1),
                    product_info=ProductInfo(
                        product_id=product_id_1,
                        observed_on=datetime.date(year=2020, month=1, day=10),
                        price=Decimal('150.99'),
                        discounted_price=None,
                        rating=Decimal('4.60'),
                        rates_count=520,
                        unit='1 kg')
                    ),
            Product(product_id=product_id_2,
                    product_code='product_id_2',
                    category_id=1,
                    name='Product 2',
                    url='url_2',
                    created_on=datetime.date(2000, 1, 1),
                    product_info=ProductInfo(
                        product_id=product_id_2,
                        observed_on=datetime.date(year=2020, month=1, day=10),
                        price=Decimal('200.00'),
                        discounted_price=None,
                        rating=Decimal('4.90'),
                        rates_count=1000,
                        unit='1 l')
                    )
        ]
    )


@pytest.fixture
def product_list_with_ids():
    return make_products()


@pytest.fixture
def product_list_without_ids():
    return make_products(without_ids=True)


def test_product_list_get_products_ids(product_list_with_ids):
    """
    test that get_products_ids method of ProductList class returns a list
    of expected ids
    """
    result = product_list_with_ids.get_products_codes()
    assert result == ['product_id_1', 'product_id_2']


def test_product_list_update_product_ids_updates(
        product_list_without_ids, product_list_with_ids):
    """
    test that update_product_ids method of product_list correctly updates
    products' ids from a provided code_map
    """
    code_map = {
        'product_id_1': 1,
        'product_id_2': 2
    }
    product_list_without_ids.update_product_ids(code_map)
    assert product_list_without_ids == product_list_with_ids


def test_product_list_update_product_ids_raises(product_list_without_ids):
    """
    test that update_product_ids method of product_list raises KeyError
    when there is no corresponding product_id for each product_code
    in a provided code_map
    """
    code_map = {
        'product_id_1': 1
    }
    with pytest.raises(KeyError):
        product_list_without_ids.update_product_ids(code_map)

