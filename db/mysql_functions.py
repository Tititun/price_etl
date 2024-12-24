"""
this module creates functions for connection to MySQL database
"""
import datetime
import os
import pickle
from typing import Literal, Optional

from dotenv import load_dotenv
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract

from scrapers.common import (Category, ProductInfo, ProductList,
                             RequestData, Supermarket, get_today_date)

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


def airflow_connect() -> MySQLConnectionAbstract:
    """
    create a MySQL connection to airflow_db using environmental variables
    :return: connection and its cursor as a tuple
    """
    connection = mysql.connector.connect(
        user=os.environ['airflow_user'],
        password=os.environ['airflow_password'],
        host=os.environ['airflow_host'],
        database=os.environ['airflow_database']
    )
    return connection


def airflow_category() -> Category:
    """
    returns Category that was fetched in the fetch_category step of
    supermarket's DAG by this DAG's run_id
    :return: Category
    """
    run_id = os.environ['run_id']
    with airflow_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    SELECT value FROM xcom
                    WHERE run_id=%s and task_id="fetch_category"
                    and `key`="return_value";
                """, (run_id, )
            )
            return pickle.loads(cursor.fetchone()[0])


def airflow_data() -> RequestData:
    """
    returns RequestData that was fetched in the fetch_data step of
    supermarket's DAG by this DAG's run_id
    :return: RequestData
    """
    run_id = os.environ['run_id']
    with airflow_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    SELECT value FROM xcom
                    WHERE run_id=%s and task_id="fetch_data"
                    and `key`="return_value";
                """, (run_id, )
            )
            return pickle.loads(cursor.fetchone()[0])


def airflow_product_list() -> ProductList:
    """
    returns ProductList that was fetched in the transform step of
    supermarket's DAG by this DAG's run_id
    :return: ProductList
    """
    run_id = os.environ['run_id']
    with airflow_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    SELECT value FROM xcom
                    WHERE run_id=%s and task_id="transform"
                    and `key`="return_value";
                """, (run_id, )
            )
            return pickle.loads(cursor.fetchone()[0])


def fetch_supermarket_by_name(connection: MySQLConnectionAbstract, name: str) \
        -> Optional[Supermarket]:
    """
    fetches a supermarket's id by its name from the database
    and makes a Supermarket object
    :param connection: MySQL connection
    :param str name: name by which to perform search
    :returns: Supermarket object or None if not found
    """
    with connection.cursor() as cur:
        cur.execute(
            'SELECT supermarket_id FROM supermarkets WHERE name=%s',
            (name,)
        )
        result = cur.fetchone()
    if result:
        return Supermarket(supermarket_id=result[0], name=name)


def fetch_supermarket_by_id(connection: MySQLConnectionAbstract, id_: int) \
        -> Optional[Supermarket]:
    """
    fetches a supermarket's name by its id from the database
    and makes a Supermarket object
    :param connection: MySQL connection
    :param str id_: id by which to perform search
    :returns: Supermarket or None if not found
    """
    with connection.cursor() as cur:
        cur.execute(
            'SELECT name FROM supermarkets WHERE supermarket_id=%s',
            (id_,)
        )
        result = cur.fetchone()
    if result:
        return Supermarket(supermarket_id=id_, name=result[0])


def fetch_category_to_scrape(connection: MySQLConnectionAbstract,
                             supermarket: Supermarket) -> Optional[Category]:
    """
    fetch a category from categories table which was updated
    more than 7 days from today
    or hasn't been scraped at all in a random order
    :param connection: MySQL connection
    :param supermarket: Supermarket object
    :return: Category object
    """
    today = get_today_date()
    with connection.cursor(dictionary=True) as cur:
        cur.execute(
            """
            SELECT 
                supermarket_id, category_id, category_code, 
                name, last_scraped_on
            FROM categories
            WHERE
                supermarket_id = %(supermarket_id)s AND
                (last_scraped_on < DATE_SUB(%(today)s, INTERVAL 6 DAY)
                 OR last_scraped_on IS NULL)
                 AND
                (last_empty_on IS NULL OR last_empty_on < %(today)s)
            ORDER BY RAND()
            LIMIT 1;
            """,
            ({'today': today, 'supermarket_id': supermarket.supermarket_id})
        )
        result = cur.fetchone()
        return Category(**result) if result else None


def fetch_supermarket_categories(
        connection: MySQLConnectionAbstract,
        supermarket: Supermarket) -> list[Category]:
    """
    fetches supermarket's categories from the database
    :param connection: MySQL connection
    :param supermarket:Supermarket object
    :returns: list of Category objects
    """
    with connection.cursor(dictionary=True) as cur:
        cur.execute(
            'SELECT supermarket_id, category_id, category_code, c.name AS name,'
            ' c.last_scraped_on'
            ' FROM categories c JOIN supermarkets s USING (supermarket_id)'
            ' WHERE s.name=%s',
            (supermarket.name,)
        )
        return [Category(**res) for res in cur.fetchall()]


def upsert_categories(connection: MySQLConnectionAbstract,
                      categories: list[Category]) -> None:
    """
    upserts new categories into the table.
    new values are inserted, existent records remain untouched (no duplicates)
    :param connection:  MySQL connection
    :param categories: list of Category objects
    """

    with connection.cursor() as cursor:
        cursor.executemany(
            """INSERT INTO categories
               (supermarket_id, category_code, name) VALUES
               (%(supermarket_id)s, %(category_code)s, %(name)s)
               ON DUPLICATE KEY UPDATE name=VALUES(name);""",
            [category.model_dump() for category in categories]
        )
        connection.commit()
    return


def fetch_products_codes(
        connection: MySQLConnectionAbstract,
        category: Category) -> set[str]:
    """
    fetches product codes of all products of the category
    :param connection: MySQL connection
    :param category: Category for which to fetch product codes
    :return: list of products' codes
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT product_code
            FROM products JOIN categories USING (category_id)
            WHERE supermarket_id=%s;
            """,
            (category.supermarket_id,)
        )
        return {res[0] for res in cursor.fetchall()}


def update_existent_products(connection: MySQLConnectionAbstract,
                             to_update: ProductList) -> None:
    """
    updates existent products in the database - their name and url
    :param connection: MySQL connection
    :param to_update: ProductList containing Product objects to update
    """
    with connection.cursor() as cursor:
        cursor.executemany(
            """UPDATE products
              SET name=%(name)s, url=%(url)s
              WHERE product_code=%(product_code)s 
                    AND category_id=%(category_id)s
           """,
            to_update.model_dump(
                exclude={'items': {'__all__': {'product_info'}}})['items']
        )
        connection.commit()


def insert_new_products(connection: MySQLConnectionAbstract,
                        to_insert: ProductList) -> None:
    """
    inserts new products into the database
    :param connection: MySQL connection
    :param to_insert: ProductList containing Product objects to insert
    """
    with connection.cursor() as cursor:
        cursor.executemany(
            """
           INSERT INTO products VALUES
              (%(product_id)s, %(product_code)s, %(category_id)s,
               %(name)s, %(url)s, %(created_on)s)
           """, to_insert.model_dump(
                exclude={'items': {'__all__': {'product_info'}}})['items']
        )
        connection.commit()


def insert_product_infos(connection: MySQLConnectionAbstract,
                         product_infos: list[ProductInfo]) -> None:
    """
    inserts product information into product_info table
    duplicates on (product_id, observed_on) will not be inserted
    :param connection: MySQL connection
    :param product_infos: list of ProductInfo to insert or update
    """
    with connection.cursor() as cursor:
        cursor.executemany(
            """
            INSERT IGNORE INTO product_info VALUES
               (%(product_id)s, %(observed_on)s, %(price)s, 
               %(discounted_price)s, %(rating)s, %(rates_count)s, %(unit)s)
            """, [info.model_dump() for info in product_infos]
        )
        connection.commit()


def fetch_product_codes_map(
        connection: MySQLConnectionAbstract,
        category: Category,
        product_list: ProductList) -> dict[str, int]:
    """
    requests product_ids for all the products in product_list and creates
    a mapping product_code -> product_id
    :param connection: MySQL connection
    :param category: category of products in product_list
    :param product_list: ProductList
    :return: dictionary with product_codes as keys, product_ids as values
    """
    supermarket_id = category.supermarket_id
    format_string = ','.join(['%s' for _ in product_list.items])
    query = f"""
            SELECT product_code, product_id
            FROM products JOIN categories USING (category_id)
            WHERE supermarket_id=%s AND product_code IN ({format_string});
            """
    with connection.cursor() as cursor:
        cursor.execute(
            query,
            (supermarket_id, *[p.product_code for p in product_list.items])
        )
        return {r[0]: r[1] for r in cursor.fetchall()}


def upsert_product_list(connection: MySQLConnectionAbstract,
                        product_list: ProductList,
                        category: Category) -> datetime.date:
    """
    updates product's name and category_id if these products already exist
    inserts new products into database if they are new
    assigns product_ids of corresponded products to product infos
    inserts product_info for each product
    :param connection: MySQL connection
    :param product_list: ProductList
    :param category: Category of all the products in product_list
    :return: returns the date of the observations
    """

    existent_product_codes = fetch_products_codes(connection, category)

    to_insert = ProductList(items=[])
    to_update = ProductList(items=[])
    for product in product_list.items:
        product.category_id = category.category_id
        if product.product_code in existent_product_codes:
            to_update.items.append(product)
        else:
            to_insert.items.append(product)

    update_existent_products(connection, to_update)
    insert_new_products(connection, to_insert)

    codes_map = fetch_product_codes_map(connection, category, product_list)
    product_list.update_product_ids(codes_map)
    product_infos = [p.product_info for p in product_list.items]
    insert_product_infos(connection, product_infos)

    return product_list.items[0].product_info.observed_on


def update_category_date_field(
        connection: MySQLConnectionAbstract,
        category: Category,
        date: datetime.date,
        field: Literal['last_scraped_on', 'last_empty_on']
    ) -> None:
    """
    updates last_scraped_on or last_empty_on of category to date
    :param connection: MySQL connection
    :param category: Category
    :param date: datetime date
    :param field: a filed to update - last_scraped_on or last_empty_on
    """
    today = get_today_date()
    date_dif = today - date
    if date_dif.days < 0:
        raise ValueError('Can\'t assign a date from the future')
    with connection.cursor() as cursor:
        cursor.execute(f"""
                       UPDATE categories
                       SET {field}=%s
                       WHERE category_id=%s
                       """,
                       (date, category.category_id))
        connection.commit()


if __name__ == '__main__':
    pass
