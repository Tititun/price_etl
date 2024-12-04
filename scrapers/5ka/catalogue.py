"""
This module scrapes the catalogue from
https://5d.5ka.ru/api/catalog/v1/stores/E703/categories

The store id is E703, which has a location:
Россия, Томск, проспект Кирова 68
"""
import logging
from pprint import pprint

import requests

from scrapers.common import headers, log_args


logger = logging.getLogger(__name__)
logging.basicConfig(**log_args, level=logging.DEBUG)

params = {
    'mode': 'delivery',
    'include_subcategories': '1',
}


def main():
    """
    makes a request for categories' ids
    then saves them to the database
    :return: None
    """
    response = requests.get(
        'https://5d.5ka.ru/api/catalog/v1/stores/E703/categories',
        params=params,
        headers=headers,
    )
    if not response.ok:
        logger.error('request to the 5ka catalogue'
                     f' failed with status code {requests.status_codes}')
        return


if __name__ == '__main__':
    main()


