"""
This module scrapes the catalogue from
https://5d.5ka.ru/api/catalog/v1/stores/E703/categories
Supermarket location is defined by SUPERMARKET_CODE constant
"""
import logging

import requests

from db.mysql_functions import (mysql_connect,
                                fetch_supermarket_by_name, upsert_categories)
from scrapers.common import Category, Supermarket, headers, log_args
from scrapers.pyaterochka.common import SUPERMARKET_NAME, SUPERMARKET_CODE

logger = logging.getLogger(__name__)
logging.basicConfig(**log_args, level=logging.DEBUG)

params = {
    'mode': 'delivery',
    'include_subcategories': '1',
}


def parse_categories(
        json_data: dict, supermarket: Supermarket) -> list[Category]:
    """
    takes json data and looks in it for the categories of the lowest level
    (those that don't have subcategories). There are only two levels
    of categories for this supermarket, so we take all the categories from the
    second level
    :param json_data: data received from the server in main function
    :param supermarket: supermarket for which we parse categories
    :return: parsed information about categories in a list of Categories
    """
    result_data = []
    for top_category in json_data:
        for subcategory in top_category['subcategories']:
            name = top_category['name'] + ' / ' + subcategory['name']
            result_data.append(
                Category(category_id=None,
                         supermarket_id=supermarket.supermarket_id,
                         category_code=subcategory['id'],
                         name=name)
            )
    return result_data


def main():
    """
    makes a request for categories' ids
    then parses them and saves to the database
    :return: None
    """
    response = requests.get(
        f'https://5d.5ka.ru/api/catalog/v1/stores/'
        f'{SUPERMARKET_CODE}/categories',
        params=params,
        headers=headers,
    )
    if not response.ok:
        logger.error('request to the 5ka catalogue'
                     f' failed with status code {response.status_code}')
        return

    with mysql_connect() as conn:
        supermarket = fetch_supermarket_by_name(conn, SUPERMARKET_NAME)
        categories = parse_categories(response.json(), supermarket)

        if not categories:
            logger.error('No categories have been fetched/parsed.')
            return

        upsert_categories(conn, categories)


if __name__ == '__main__':
    main()
