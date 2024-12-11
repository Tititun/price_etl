"""
this module creates functions for connection to MySQL database
"""
import os
from typing import Optional

from dotenv import load_dotenv
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract

from scrapers.common import (Category, Product, ProductInfo,
                             ProductList, get_today_date)

load_dotenv()


def mysql_connect() -> MySQLConnectionAbstract:
    """
    create a MySQL connection using environmental variables
    :return: connection and its cursor as a tuple
    """
    connection = mysql.connector.connect(user=os.environ['mysql_user'],
                                         password=os.environ['mysql_password'],
                                         host=os.environ['mysql_host'],
                                         database=os.environ['mysql_database'])
    return connection


def fetch_supermarket_id(connection: MySQLConnectionAbstract, name: str)\
        -> Optional[int]:
    """
    fetches a supermarket's id by its name from the database
    :param connection: MySQL connection
    :param str name: name of the supermarket
    :returns: id of the supermarket or None if not found
    """
    with connection.cursor() as cur:
        cur.execute(
            'SELECT supermarket_id FROM supermarkets WHERE name=%s',
            (name,)
        )
        result = cur.fetchone()
    return result[0] if result else None


def fetch_supermarket_name(connection: MySQLConnectionAbstract, id_: int)\
        -> Optional[str]:
    """
    fetches a supermarket's name by its id from the database
    :param connection: MySQL connection
    :param str id_: id of the supermarket
    :returns: name of the supermarket or None if not found
    """
    with connection.cursor() as cur:
        cur.execute(
            'SELECT name FROM supermarkets WHERE supermarket_id=%s',
            (id_,)
        )
        result = cur.fetchone()
    return result[0] if result else None


def fetch_category_to_scrape(connection: MySQLConnectionAbstract,
                             supermarket_name: str) -> Optional[Category]:
    """
    fetch a category from categories table which was updated
    more than 7 days ago or hasn't been scraped at all in a random order
    :param connection: MySQL connection
    :param supermarket_name: name of the supermarket
    :return: Category object
    """
    today = get_today_date()
    with connection.cursor(dictionary=True) as cur:
        cur.execute(
            """
            SELECT 
                c.supermarket_id, c.category_id, c.name, c.last_scraped_on
            FROM categories c JOIN supermarkets s USING (supermarket_id)
                WHERE
                s.name = %(supermarket_name)s AND
                (last_scraped_on < DATE_SUB(%(today)s, INTERVAL 6 DAY)
                 OR last_scraped_on IS NULL)
            ORDER BY RAND()
            LIMIT 1;
            """,
            ({'today': today, 'supermarket_name': supermarket_name})
        )
        result = cur.fetchone()
        return Category(**result) if result else None


def fetch_supermarket_categories(
        connection: MySQLConnectionAbstract, name: str) -> list[Category]:
    """
    fetches supermarket's categories using its name from the database
    :param connection: MySQL connection
    :param str name: name of the supermarket
    :returns: list of Category objects
    """
    with connection.cursor(dictionary=True) as cur:
        cur.execute(
            'SELECT supermarket_id, category_id, c.name AS name,'
            ' c.last_scraped_on'
            ' FROM categories c JOIN supermarkets s USING (supermarket_id)'
            ' WHERE s.name=%s',
            (name,)
        )
        return [Category(**res) for res in cur.fetchall()]


def upsert_categories(
    connection: MySQLConnectionAbstract,
    categories: list[Category],
    supermarket: str,
) -> None:
    """
    upserts new categories into the table.
    new values are inserted, existent records remain untouched (no duplicates)
    :param connection:  MySQL connection
    :param categories: list of Category objects
    :param supermarket: name of the supermarket
    """
    supermarket_id = fetch_supermarket_id(connection, name=supermarket)
    for category in categories:
        category.supermarket_id = supermarket_id

    with connection.cursor() as cursor:
        cursor.executemany(
            'INSERT IGNORE INTO categories '
            '(supermarket_id, category_id, name) VALUES'
            '(%(supermarket_id)s, %(category_id)s, %(name)s);',
           [category.model_dump() for category in categories]
        )
        connection.commit()
    return


def fetch_products_ids(
        connection: MySQLConnectionAbstract, category: Category) -> list[str]:
    """
    fetches ids of all products that are releated to Category
    :param connection: MySQL connection
    :param category: Category
    :return: list of products' ids
    """
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT product_id FROM products '
            'WHERE supermarket_id=%s AND category_id=%s;',
            (category.supermarket_id, category.category_id)
        )
        return [res[0] for res in cursor.fetchall()]



def upsert_product_list(connection: MySQLConnectionAbstract,
                        product_list: ProductList) -> None:
    """
    inserts new products into database if they are new
    updates product's name and category_id if these products already exist
    upserts product_info for each product
    :param connection: MySQL connection
    :param product_list: ProductList
    """


if __name__ == '__main__':
    with mysql_connect() as con:
        print(fetch_category_to_scrape(con, 'Пятёрочка'))
