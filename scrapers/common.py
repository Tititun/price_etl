"""
This file contains common objects which can be used by many scrapers
"""

import datetime
from decimal import Decimal
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


class Category(BaseModel):
    """
    this class is used to validate category data which
    is collected by scrapers.
    """
    supermarket_id: Optional[int]
    category_id: str
    name: str


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
    category_id: int
    name: str
    created_on: datetime.date
    product_info: Optional[ProductInfo] = None
