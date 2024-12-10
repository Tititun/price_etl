"""
This file contains common objects which can be used by many scrapers
"""

import datetime
from decimal import Decimal
import pytz
from typing import Optional

from pydantic import BaseModel


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


class Category(BaseModel):
    """
    this class is used to validate category data which
    is collected by scrapers.
    """
    supermarket_id: Optional[int]
    category_id: str
    name: str
    last_scraped_on: Optional[datetime.date] = None


class ProductInfo(BaseModel):
    """
    this class represents product info which is associated with a product
    """
    product_id: str
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
    product_id: str
    supermarket_id: int
    category_id: str
    name: str
    created_on: datetime.date
    product_info: Optional[ProductInfo] = None


class ProductList(BaseModel):
    """
    this class represents a list of Product instances
    """
    items: list[Product]
