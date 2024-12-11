"""
this module contains the main scraper for the items in "Пятёрочка" supermarket
"""
import datetime
import logging
from typing import Optional

import requests

from scrapers.common import (Category, Product, ProductInfo, ProductList,
                             RequestData, get_today_date, headers, log_args)
from scrapers.pyaterochka.common import SUPERMARKET_CODE, SUPERMARKET_NAME

logger = logging.getLogger(__name__)
logging.basicConfig(**log_args, level=logging.DEBUG)


def request_data(category_id: str) -> RequestData:
    """
    requests all the products for the given category id and returns them
    as json data
    :param str category_id: id of the category taken from the database
    :return: products' information as a list
    """
    params = {
        'mode': 'delivery',
        'limit': '100',
    }
    response = requests.get(
        f'https://5d.5ka.ru/api/catalog/v1/stores/{SUPERMARKET_CODE}/'
        f'categories/{category_id}/products',
        params=params,
        headers=headers,
    )
    if response.ok:
        return RequestData(data={"products": response.json()['products']},
                           date=get_today_date())

    else:
        logger.error(f'request to 5ka failed with '
                     f'status code {response.status_code}')


def parse_data(raw_data: list) -> list[Product]:
    """
    takes in the data returned by request to the server and parses all the
    products with their prices from it
    :param raw_data: json data as a dictionary
    :return: ProductList - validated list of Product objects
    """
    # data = request_data(category.category_id)
    # date = datetime.date.today()
    # results = []
    # for record in data:
    #     product_id = str(record['plu'])
    #     product = Product(
    #         product_id=product_id,
    #         supermarket_id=category.supermarket_id,
    #         category_id=category.category_id,
    #         name=record['name'],
    #         created_on=date
    #     )
    #     rating_info = record.get('rating') or {}
    #     product_info = ProductInfo(
    #         product_id=product_id,
    #         observed_on=date,
    #         price=record['prices']['regular'],
    #         discounted_price=record['prices']['discount'],
    #         rating=rating_info.get('rating_average'),
    #         rates_count=rating_info.get('rates_count') or 0,
    #         unit=record['property_clarification']
    #     )
    #     product.product_info = product_info
    #     results.append(product)
    # return results


if __name__ == '__main__':
    from pprint import pprint
    # data = scrape_category(Category(supermarket_id=1, category_id='73C9746', name='test'))
    # pprint(data)
    import pytz
    print(datetime.datetime.now(tz=pytz.timezone('Asia/Tomsk')).date())
    # datetime.datetime()
