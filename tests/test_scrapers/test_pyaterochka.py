"""
this module contains tests that cover functions from pyaterochka scraper
"""

import json
from pathlib import Path

import pytest
import requests

from scrapers.common import Category, RequestData, get_today_date
from scrapers.pyaterochka.catalogue import parse_categories
from scrapers.pyaterochka.scraper import request_data


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
        Category(supermarket_id=None, category_id='73C20455',
                 name='Готовим оливье'),
        Category(supermarket_id=None, category_id='73C20456',
                 name='Запекаем в духовке'),
        Category(supermarket_id=None, category_id='73C10301',
                 name='Горячие напитки'),
        Category(supermarket_id=None, category_id='73C9714',
                 name='Блинчики, сырники и каши'),
    ]
    parsed_categories = parse_categories(example_data)
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


def test_request_data(monkeypatch):
    """
    test that request_data returns a list when response is successful
    """
    def mock_response(*args, **kwargs):
        return MockResponse(200)

    monkeypatch.setattr(requests, 'get', mock_response)
    expected_result = RequestData(data={"products": []}, date=get_today_date())

    assert request_data('some_category_id') == expected_result


def test_request_data_fails(monkeypatch):
    """
    test that request_date returns None if request is not successful
    """
    def mock_response(*args, **kwargs):
        return MockResponse(403)

    monkeypatch.setattr(requests, 'get', mock_response)

    assert request_data('some_category_id') is None
