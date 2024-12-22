"""
This module scrapes the catalogue from
https://lenta.com/
Supermarket location is set to: Томск, Елизаровых ул., 13
The locations is tied to SessionToken in the headers:
SessionToken - 00BAEB8C9C2BCBE317F8340311FD1E74
"""
import logging

import requests

from db.mysql_functions import (mysql_connect,
                                fetch_supermarket_by_name, upsert_categories)
from scrapers.common import Category, Supermarket, log_args
from scrapers.lenta.common import headers, SUPERMARKET_NAME

logger = logging.getLogger(__name__)
logging.basicConfig(**log_args, level=logging.DEBUG)


def parse_categories(
        json_data: dict, supermarket: Supermarket) -> list[Category]:
    """
    takes json data and looks in it for the categories of the lowest level
    (those that don't have subcategories). There are only 3 levels
    of categories for this supermarket, and we take all the categories from the
    second level, excluding those whose parents' names are in
    excluded_parents list
    :param json_data: data received from the server in main function
    :param supermarket: supermarket for which we parse categories
    :return: parsed information about categories in a list of Categories
    """
    excluded_parents = ['Особенно выгодно', 'Новинки', 'Новогодний стол',
                        'Новогодний декор и подарки', 'Особенно выгодно',
                        'Товары до 99 рублей']
    result_data = []
    for category in json_data['categories']:
        if category['level'] == 2:
            if category['parentName'] in excluded_parents:
                continue
            if category['parentName'].startswith('Каталог выгодных'):
                continue
            name = category['parentName'] + ' / ' + category['name']
            result_data.append(
                Category(category_id=None,
                         supermarket_id=supermarket.supermarket_id,
                         category_code=str(category['id']),
                         name=name)
            )
    return result_data


def main():
    """
    makes a request for categories' catalogue
    then parses them and saves to the database
    :return: None
    """
    response = requests.get(
        f'https://lenta.com/api-gateway/v1/catalog/categories',
        headers=headers,
    )
    if not response.ok:
        logger.error('request to the lenta catalogue'
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
