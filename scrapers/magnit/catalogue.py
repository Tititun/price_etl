"""
This module scrapes the catalogue from
https://magnit.ru
Supermarket location is г. Томск, ул. Киевская, д. 105б
"""
import json
import logging
import re

from bs4 import BeautifulSoup
import requests

from db.mysql_functions import (mysql_connect,
                                fetch_supermarket_by_name, upsert_categories)
from scrapers.common import headers, log_args, Supermarket, Category
from scrapers.magnit.common import cookies, SUPERMARKET_NAME

logger = logging.getLogger(__name__)
logging.basicConfig(**log_args, level=logging.DEBUG)


def collect_category_info(json_data: list) -> dict[int, dict]:
    """
    looks in the list for category information and returns it in the form of
    a dictionary where keys are categories' ids and values are dictionaries
    with name, parent_id and children keys
    :param json_data: data received from the server
    :return: dictionary of category data
    """
    collected_info = {}
    for item in json_data:
        if not isinstance(item, str):
            continue
        match = re.match(r'^g\d+$', item)
        if match:
            category_index = json_data.index(item)
            category_id = (int(item[1:]))
            info_dict = json_data[category_index - 1]
            if not isinstance(info_dict, dict):
                continue
            else:
                details = {}
                details['name'] = json_data[info_dict['name']].strip()
                details['parent_id'] = json_data[info_dict['parentKey']][1:]
                details['parent_id'] = int(details['parent_id'])
                details['children'] = json_data[info_dict['children']]
                collected_info[category_id] = details
    return collected_info


def construct_full_name(category_info: dict[int, dict]) -> dict[int, dict]:
    """
    for each item in category_info searches recursively for its parents
    in category_info and constructs its full name in the form of
    parent 1 / parent 2 / ... / parent n
    :param dict: dictionary of categories' data
    :return: dictionary of categories' data with full_name values added to it
    """
    def get_parents(id_, result=[]):
        result = result or [id_]
        parent_id = category_info[id_]['parent_id']
        if not parent_id or parent_id == id_:
            return result
        else:
            result.insert(0, parent_id)
            return get_parents(parent_id, result)

    for category_id in category_info:
        category_info[category_id]['parent'] = get_parents(category_id)

    for category, details in category_info.items():
        full_name = (
            ' / '.join([category_info[idx]['name']
            for idx in details['parent']])
        )
        details['full_name'] = full_name

    category_info = {
        c: v for c, v in category_info.items()
        if len(v['parent']) > 1 and not v['children']
    }
    return category_info


def parse_categories(
        json_data: list, supermarket: Supermarket) -> list[Category]:
    """
    takes json data and looks in it for the categories of the lowest level
    (those that don't have subcategories).
    :param json_data: data received from the server in main function
    :param supermarket: supermarket for which we parse categories
    :return: parsed information about categories in a list of Categories
    """
    categories_info = collect_category_info(json_data)
    categories_info = construct_full_name(categories_info)

    to_exclude = re.compile(r'^Не забудьте заказать|^Покупайте с выгодой|'
                            r'^Промокод Магнит|^Создаём праздник')

    result_data = []
    for category_id, details in categories_info.items():
        full_name = details['full_name']
        if to_exclude.search(full_name):
            continue
        result_data.append(
            Category(category_id=None,
                     supermarket_id=supermarket.supermarket_id,
                     category_code=str(category_id),
                     name=full_name)
        )
    return result_data


def main():
    """
    makes a request for categories' ids
    then parses them and saves to the database
    :return: None
    """
    response = requests.get('https://magnit.ru/',
                            headers=headers, cookies=cookies)
    if not response.ok:
        logger.error('request to the magnit catalogue'
                     f' failed with status code {response.status_code}')
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    script = soup.select_one('#__NUXT_DATA__')
    if not script:
        logger.error('No script data found')

    script = json.loads(script.text)

    with mysql_connect() as conn:
        supermarket = fetch_supermarket_by_name(conn, SUPERMARKET_NAME)
        categories = parse_categories(script, supermarket)

        if not categories:
            logger.error('No categories have been fetched/parsed.')
            return

        upsert_categories(conn, categories)


if __name__ == '__main__':
    main()
