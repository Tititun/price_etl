"""
this module contains the main scraper for the items in "Пятёрочка" supermarket
"""
import logging
from typing import Optional

import requests

from db.mysql_functions import fetch_supermarket_id
from scrapers.common import Category, Product, ProductInfo, headers, log_args
from scrapers.pyaterochka.common import SUPERMARKET_CODE

logger = logging.getLogger(__name__)
logging.basicConfig(**log_args, level=logging.DEBUG)


def request_data(inner_code: str) -> Optional[list]:
    """
    requests all the products for the given category id and returns them
    as json data
    :param str inner_code: code of the category taken from the database
    :return: products' information as a list
    """
    params = {
        'mode': 'delivery',
        'limit': '100',
    }
    response = requests.get(
        f'https://5d.5ka.ru/api/catalog/v1/stores/{SUPERMARKET_CODE}/'
        f'categories/{inner_code}/products',
        params=params,
        headers=headers,
    )
    if response.ok:
        return response.json()['products']
    else:
        logger.error(f'request to 5ka failed with '
                     f'status code {response.status_code}')


def scrape_category(category: Category) -> dict[Product, ProductInfo]:
    """
    scrapes a category from the supermarket's website as a dictionary
    with Product keys and ProductInfo values
    :param category: Category from categories table
    :return: dictionary mapping of each scraped product to its info
    """
    data = request_data(category.inner_code)


if __name__ == '__main__':
    pass
