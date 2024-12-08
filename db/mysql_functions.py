"""
this module creates functions for connection to MySQL database
"""
import os
from typing import Optional

from dotenv import load_dotenv
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract

from scrapers.common import Category

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
            'SELECT supermarket_id, inner_code, c.name AS name'
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
            '(supermarket_id, inner_code, name) VALUES'
            '(%(supermarket_id)s, %(inner_code)s, %(name)s);',
           [category.model_dump() for category in categories]
        )
        connection.commit()
    return


if __name__ == '__main__':
    connection = mysql_connect()
    print(fetch_supermarket_id(connection, 'Пятёрочка'))
    connection.close()
