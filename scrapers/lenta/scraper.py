"""
this module contains the main scraper for the items in "Лента" supermarket
"""
import logging

import requests

from scrapers.common import (Category, Product, ProductInfo, ProductList,
                             RequestData, get_today_date, log_args, parse_price)
from scrapers.lenta.common import headers
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
        'categoryId': int(category.category_code),
        'limit': 200,
        'offset': 0,
        'sort': {
            'type': 'popular',
            'order': 'desc',
        },
        'filters': {
            'range': [],
            'checkbox': [],
            'multicheckbox': [],
        },
    }
    response = requests.post(
        'https://lenta.com/api-gateway/v1/catalog/items',
        headers=headers, json=json_data
    )
    if response.ok:
        return RequestData(category=category,
                           data={"products": response.json()['items']},
                           date=get_today_date())

    else:
        logger.error(f'request to lenta failed with '
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
        product = Product(
            product_id=None,
            product_code=product_code,
            category_id=category.category_id,
            name=record['name'],
            url=f'https://lenta.com/product/{record["slug"]}-{product_code}',
            created_on=request_data.date
        )
        rating_info = record.get('rating') or {}
        product_info = ProductInfo(
            product_id=None,
            observed_on=request_data.date,
            price=parse_price(record['prices'], 'priceRegular', unit='k'),
            discounted_price=parse_price(record['prices'], 'price', unit='k'),
            rating=rating_info.get('rate'),
            rates_count=rating_info.get('votes') or 0,
            unit=record.get('weight', {}).get('package')
        )
        product.product_info = product_info
        product_list.items.append(product)
    return product_list


if __name__ == '__main__':
    category_ = Category(supermarket_id=2, category_id=1,
                         name='c', category_code='1033')
    data = request_data(category_)
    parsed_data = parse_data(data, category_)
    print(parsed_data)
