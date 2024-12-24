"""
This file contains common objects which can be used by many scrapers
"""

import datetime
from decimal import Decimal
import os
from pathlib import Path
import pytz
from typing import Literal, Optional

from dotenv import load_dotenv
from pydantic import BaseModel
import requests


# headers to use in requests
headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0)'
                  ' Gecko/20100101 Firefox/133.0',
}

# default logger arguments
log_args = {
    'format': '%(levelname)s %(filename)s %(asctime)s: %(message)s',
    'datefmt': '%Y-%m-%d %H:%M:%S',
}


def get_today_date() -> datetime.date:
    """
    gets current date for 'Asia/Tomsk' timezone
    :return: datetime.date
    """
    return datetime.datetime.now(tz=pytz.timezone('Asia/Tomsk')).date()


def parse_price(
        item: dict, field_name: str, unit: Literal['r', 'k'] = 'r'
        ) -> Optional[Decimal]:
    """
    parses field_name from item, trying to convert it to Decimal if it's not
    None. Returns Decimal price in roubles (konverts to roubles from kopecks
    if unit is 'k')
    :param item: dictionary
    :param field_name: string - key to get from item dictionary
    :param unit: the unit of input data (r - roubles, k - kopecks)
    :return: Decimal if field_name in item, else None
    """
    value = item.get(field_name)
    if value is None:
        return
    else:
        if unit == 'k':
            return Decimal(str(value)) / 100
        return Decimal(str(value))


class Supermarket(BaseModel):
    """class to store supermarket data"""
    supermarket_id: int
    name: str


class Category(BaseModel):
    """
    this class is used to validate category data which
    is collected by scrapers.
    """
    category_id: Optional[int]
    supermarket_id: int
    category_code: str
    name: str
    last_scraped_on: Optional[datetime.date] = None
    last_empty_on: Optional[datetime.date] = None


class ProductInfo(BaseModel):
    """
    this class represents product info which is associated with a product
    """
    product_id: Optional[int]
    observed_on: datetime.date
    price: Optional[Decimal]
    discounted_price: Optional[Decimal]
    rating: Optional[Decimal]
    rates_count: int = 0
    unit: Optional[str]


class Product(BaseModel):
    """
    this class represents a product
    """
    product_id: Optional[int]
    product_code: str
    category_id: int
    name: str
    url: str
    created_on: datetime.date
    product_info: Optional[ProductInfo] = None


class ProductList(BaseModel):
    """
    this class represents a list of Product instances
    """
    items: list[Product]

    def get_products_codes(self) -> list[str]:
        return [product.product_code for product in self.items]

    def update_product_ids(self, code_map: dict[str, int]) -> None:
        """
        assign product_id from code_map to each product and product info
        in items
        """
        for product in self.items:
            code = product.product_code
            product_id = code_map.get(code)
            if not product_id:
                raise KeyError(f'No product_id found for {code}')
            product.product_id = product_id
            product.product_info.product_id = product_id

    def __bool__(self):
        return len(self.items) > 0


class RequestData(BaseModel):
    """
    this class represents data returned by requests function where data is the
    data received from the endpoint,
    date is the date of request,
    category - category for which request was made
    """
    category: Category
    data: dict
    date: datetime.date


def telegram_callback_on_failure(context: dict):
    """
    send a telegram error message upon a DAG failure
    :context: dictionary with context from the failed DAG
    """

    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)
    dag_id = context['task_instance'].dag_id
    task_id = context['task_instance'].task_id
    message = f'TASK {task_id} FAILED IN DAG {dag_id}'
    requests.get(f"https://api.telegram.org/bot"
                 f"{os.environ['TELEGRAM_BOT_TOKEN']}"
                 f"/sendMessage?"
                 f"chat_id={os.environ['TELEGRAM_BOT_CHAT_ID']}&"
                 f"text={message}",
                 timeout=10)
