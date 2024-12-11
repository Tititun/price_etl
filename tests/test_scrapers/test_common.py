"""
this module contains tests that cover functions from scrapers' common module
"""
from decimal import Decimal
from scrapers.common import parse_price


def test_parse_price_none():
    """test if parse_price returns None when value is None"""
    assert parse_price({'price': None}, 'price') is None


def test_parse_price_decimal():
    """test if parse_price returns Decimal when value is not None"""
    assert parse_price({'price': 12.99}, 'price') == Decimal('12.99')
