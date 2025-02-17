"""
this module contains the main scraper for the items in "Пятёрочка" supermarket
"""
import datetime
import logging

import requests

from scrapers.common import (Category, Product, ProductInfo, ProductList,
                             RequestData, get_today_date, headers, log_args,
                             parse_price)
from scrapers.pyaterochka.common import SUPERMARKET_CODE

logger = logging.getLogger(__name__)
logging.basicConfig(**log_args, level=logging.DEBUG)


def request_data(category: Category) -> RequestData:
    """
    requests all the products for the given category and returns them as
    RequestData
    :param category: Category to scrape taken from the database
    :return: RequestData
    """
    params = {
        'mode': 'delivery',
        'limit': '100',
    }
    response = requests.get(
        f'https://5d.5ka.ru/api/catalog/v1/stores/{SUPERMARKET_CODE}/'
        f'categories/{category.category_code}/products',
        params=params,
        headers=headers,
    )
    if response.ok:
        return RequestData(category=category,
                           data={"products": response.json()['products']},
                           date=get_today_date())

    else:
        logger.error(f'request to 5ka failed with '
                     f'status code {response.status_code}')


def parse_data(request_data: RequestData, category: Category) -> ProductList:
    """
    takes in the data returned by request to the server and parses all the
    products with their prices from it
    :param request_data: data as a RequestData object
    :param category: Category of to-be-parsed items
    :return: ProductList - validated list of Product objects
    """
    product_list = ProductList(items=[])
    for record in request_data.data["products"]:
        product_code = str(record['plu'])
        product = Product(
            product_id=None,
            product_code=product_code,
            category_id=category.category_id,
            name=record['name'],
            url=f'https://5ka.ru/product/{product_code}',
            created_on=request_data.date
        )
        rating_info = record.get('rating') or {}
        product_info = ProductInfo(
            product_id=None,
            observed_on=request_data.date,
            price=parse_price(record['prices'], 'regular'),
            discounted_price=parse_price(record['prices'], 'discount'),
            rating=rating_info.get('rating_average'),
            rates_count=rating_info.get('rates_count') or 0,
            unit=record['property_clarification']
        )
        product.product_info = product_info
        product_list.items.append(product)
    return product_list


if __name__ == '__main__':
    from pprint import pprint
    # data = scrape_category(Category(supermarket_id=1, category_id='73C9746', name='test'))
    # pprint(data)
    import pytz
    print(datetime.datetime.now(tz=pytz.timezone('Asia/Tomsk')).date())
    # datetime.datetime()
