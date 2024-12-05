"""
This file contains common objects which can be used by many scrapers
"""

import logging
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
    It is also used when fetching the data from the database.
    """
    supermarket_id: Optional[int]
    category_id: Optional[int]
    inner_code: str
    name: str
