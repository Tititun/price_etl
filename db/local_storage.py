"""
This module contains functions to store json data locally.
In the future it's better to move to S3 storage.
"""
import json
import logging
import os
from pathlib import Path

from scrapers.common import Product


# local storage - a place to store parsed scraped data before commiting it
# to the database.
LOCAL_STORAGE_PATH = Path(__file__).parent.parent / 'storage'

logger = logging.getLogger(__name__)


def construct_storage_path(supermarket_name: str, product: Product) -> Path:
    """
    constructs a path store parsed product data
    makes all the necessary directories
    :param supermarket_name: supermarket's name
    :param product: an instance of Product
    :return: path to store Product data
    """
    observed_on = str(product.product_info.observed_on)
    directory_path = LOCAL_STORAGE_PATH / observed_on / supermarket_name
    os.makedirs(directory_path, exist_ok=True)
    return directory_path / f'{product.category_id}.json'


def save_to_local_storage(name: str, data: list[Product]) -> None:
    """
    takes data and stores it in the local storage using the path:
    LOCAL_STORAGE_PATH/date/supermarket/category/
    :param name: name of the supermarket
    :param data: parsed product data as a list of Product objects
    :return: None
    """
    if not data:
        logger.warning(f'provided data list is empty')
        return
    logger.debug(f'got a list of {len(data)} products')

    path = construct_storage_path(name, data[0])

    data = [item.model_dump_json(indent=4) for item in data]
    with open(path, 'w') as f:
        json.dump(data, f)
