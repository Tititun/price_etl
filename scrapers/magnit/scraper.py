"""
this module contains the main scraper for the items in "Магнит" supermarket
"""
import datetime
import logging

import requests

from scrapers.common import (Category, Product, ProductInfo, ProductList,
                             RequestData, get_today_date, headers, log_args,
                             parse_price)
from scrapers.magnit.common import SHOP_ID

logger = logging.getLogger(__name__)
logging.basicConfig(**log_args, level=logging.DEBUG)


def request_data(category: Category) -> RequestData:
    """
    requests all the products for the given category and returns them as
    RequestData
    :param category: Category to scrape taken from the database
    :return: RequestData
    """
    json_data = {
        'sort': {
            'order': 'desc',
            'type': 'popularity',
        },
        'pagination': {
            'limit': 50,
            'offset': 0,
        },
        'categories': [int(category.category_code)],
        'includeAdultGoods': True,
        'storeCode': SHOP_ID,
        'storeType': '1',
        'catalogType': '1',
    }
    response = requests.post(
        'https://magnit.ru/webgate/v2/goods/search', headers=headers,
        json=json_data
    )
    if response.ok:
        return RequestData(category=category,
                           data={"products": response.json()['items']},
                           date=get_today_date())

    else:
        logger.error(f'request to magnit failed with '
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
        product_code = str(record['id'])
        shop = record['storeCode']
        product = Product(
            product_id=None,
            product_code=product_code,
            category_id=category.category_id,
            name=record['name'],
            url=f'https://magnit.ru/product/{product_code}?shopCode={703059}',
            created_on=request_data.date
        )
        rating_info = record.get('ratings') or {}
        is_promotion = record['promotion']['isPromotion']
        if is_promotion:
            price = parse_price(record['promotion'], 'oldPrice', unit='k')
            disc_price = parse_price(record, 'price', unit='k')
        else:
            price = parse_price(record, 'price', unit='k')
            disc_price = None
        product_info = ProductInfo(
            product_id=None,
            observed_on=request_data.date,
            price=price,
            discounted_price=disc_price,
            rating=rating_info.get('rating') or None,
            rates_count=rating_info.get('scoresCount') or 0,
            unit=None
        )
        product.product_info = product_info
        product_list.items.append(product)
    return product_list


if __name__ == '__main__':
    # from pprint import pprint
    category = Category(supermarket_id=3, category_id=1699,
                        category_code='4851',
                        name='Молоко, яйца, сыр / Сыры / Твёрдые')
    data = request_data(category)
    data = parse_data(data, category)
    from pprint import pprint
    pprint(data)
