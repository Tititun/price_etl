"""
This module scrapes the catalogue from
https://5d.5ka.ru/api/catalog/v1/stores/E703/categories

The store id is E703, which has a location:
Россия, Томск, проспект Кирова 68
"""
import json
import logging

import requests

from db.mysql_functions import mysql_connect, upsert_categories
from scrapers.common import Category, headers, log_args
from scrapers.pyaterochka.common import SUPERMARKET

logger = logging.getLogger(__name__)
logging.basicConfig(**log_args, level=logging.DEBUG)

params = {
    'mode': 'delivery',
    'include_subcategories': '1',
}


def parse_categories(json_data: dict) -> list[Category]:
    """
    takes json data and looks in it for the categories of the lowest level
    (those that don't have subcategories). There are only two levels
    of categories for this supermarket, so we take all the categories from the
    second level
    :param json_data: data received from the server in main function
    :return: parsed information about categories in a list of Categories
    """
    result_data = []
    for top_category in json_data:
        for subcategory in top_category['subcategories']:
            result_data.append(Category(supermarket_id=None,
                                        category_id=None,
                                        inner_code=subcategory['id'],
                                        name=subcategory['name']))
    return result_data


def main():
    """
    makes a request for categories' ids
    then parses them and saves to the database
    :return: None
    """
    response = requests.get(
        'https://5d.5ka.ru/api/catalog/v1/stores/E703/categories',
        params=params,
        headers=headers,
    )
    if not response.ok:
        logger.error('request to the 5ka catalogue'
                     f' failed with status code {response.status_code}')
        return

    with open('example.json', 'w') as f:
        json.dump(response.json()[:2], f, ensure_ascii=False, indent=4)
    categories = parse_categories(response.json())
    if not categories:
        logger.error('No categories have been fetched/parsed.')
        return

    with mysql_connect() as conn:
        upsert_categories(conn, categories, supermarket=SUPERMARKET)


if __name__ == '__main__':
    main()
