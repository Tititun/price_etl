"""
this module contains tests that cover database operations
"""
import datetime
from decimal import Decimal
import os
from pathlib import Path

from dotenv import load_dotenv
import mysql.connector
import pytest

from db.mysql_functions import (fetch_products_ids,
                                fetch_category_to_scrape,
                                fetch_supermarket_categories,
                                fetch_supermarket_id,
                                fetch_supermarket_name,
                                upsert_categories)
from scrapers.common import Category, Product, ProductInfo, get_today_date

load_dotenv()
today = get_today_date()
ten_days_ago = today - datetime.timedelta(days=10)

# SQL queries to execute during the setup in db_connection fixture
setup_queries = [
    'INSERT INTO supermarkets VALUES (1, "First Supermarket"),'
    ' (2, "Second Supermarket");',

    'INSERT INTO categories '
    '(supermarket_id, category_id, name, last_scraped_on) '
    'VALUES '
    '(1, "test_id_1", "First Category", null),'
    f'(1, "test_id_2", "Second Category", "{today}"),'
    f'(2, "test_id_3", "Third Category", "{ten_days_ago}");',
    
    'INSERT INTO products '
    '(product_id, supermarket_id, category_id, name, created_on) '
    'VALUES '
    '("product_id_1", 1, "test_id_1", "Product 1", "2020-01-01"),'
    '("product_id_2", 1, "test_id_1", "Product 2", "2020-01-02"),'
    '("product_id_3", 1, "test_id_2", "Product 3", "2020-01-03")'
]


@pytest.fixture
def db_connection():
    """
    creates tables in the database from db/sql/create_tables.sql file
    the following empty tables should be created as a result:
        -supermarkets
        -categories
    yields a connector to test_database
    closes the connector after a test
    """
    connection = mysql.connector.connect(user=os.environ['mysql_user'],
                                         password=os.environ['mysql_password'],
                                         host=os.environ['mysql_host'],
                                         database='test_database')
    cursor = connection.cursor()
    with open(Path(__file__).parent.parent / 'db/sql/create_tables.sql') as f:
        statements = f.read().split(';')
        for statement in statements:
            cursor.execute(statement)
            connection.commit()
    for query in setup_queries:
        cursor.execute(query)
        connection.commit()
    yield connection
    cursor.execute('DROP TABLE IF EXISTS'
                   ' supermarkets, categories, products, product_info;')
    connection.commit()
    cursor.close()
    connection.close()


def test_fetch_supermarket_id_fetches(db_connection):
    """test if fetch_supermarket_id returns id for an existent supermarket"""
    result = fetch_supermarket_id(db_connection, 'First Supermarket')
    assert result == 1


def test_fetch_supermarket_id_fetches_none(db_connection):
    """
    test if fetch_supermarket_id returns None
    for a non-existent supermarket
    """
    result = fetch_supermarket_id(db_connection, 'Imaginary Supermarket')
    assert result is None


def test_fetch_supermarket_name(db_connection):
    """
    test if fetch_supermarket_name returns name for an existent supermarket
    """
    result = fetch_supermarket_name(db_connection, 1)
    assert result == 'First Supermarket'


def test_fetch_supermarket_categories(db_connection):
    """
    test if fetch_supermarket_categories returns a list of expected Category
    objects
    """
    result = fetch_supermarket_categories(db_connection, 'First Supermarket')
    expected = [
        Category(supermarket_id=1, category_id='test_id_1',
                 name='First Category'),
        Category(supermarket_id=1, category_id='test_id_2',
                 name='Second Category', last_scraped_on=today),
    ]
    assert result == expected


def test_upsert_categories_new_categories(db_connection):
    """
    test if new categories are inserted successfully
    """
    categories_to_insert = [
        Category(
            supermarket_id=None, category_id='test_id_4',
            name='Fourth Category'),
        Category(
            supermarket_id=None, category_id='test_id_5',
            name='Fifth Category'),
    ]
    expected_result = [
        Category(
            supermarket_id=2, category_id='test_id_3',
            name='Third Category', last_scraped_on=ten_days_ago),
        Category(
            supermarket_id=2, category_id='test_id_4',
            name='Fourth Category'),
        Category(
            supermarket_id=2, category_id='test_id_5',
            name='Fifth Category'),
    ]
    supermarket = "Second Supermarket"
    upsert_categories(db_connection, categories_to_insert,
                      supermarket=supermarket)
    categories = fetch_supermarket_categories(db_connection, supermarket)
    assert categories == expected_result


def test_upsert_categories_no_duplicates(db_connection):
    """
    test that categories with duplicate category_id are not inserted
    """
    categories_to_insert = [
        Category(
            supermarket_id=None, category_id='test_id_3',
            name='Third Category Different', last_scraped_on=ten_days_ago),
        Category(
            supermarket_id=None, category_id='test_id_4',
            name='Fourth Category'),
    ]
    expected_result = [
        Category(
            supermarket_id=2, category_id='test_id_3',
            name='Third Category', last_scraped_on=ten_days_ago),
        Category(
            supermarket_id=2, category_id='test_id_4',
            name='Fourth Category'),
    ]
    supermarket = "Second Supermarket"
    upsert_categories(db_connection, categories_to_insert,
                      supermarket=supermarket)
    categories = fetch_supermarket_categories(db_connection, supermarket)
    assert categories == expected_result


def test_fetch_category_to_scrape_none(db_connection):
    """
    test that fetch_category_to_scrape fetches a category with
    last_scraped_on = None
    """
    expected_result = Category(
        supermarket_id=1,
        category_id='test_id_1',
        name='First Category',
        last_scraped_on=None
    )
    result = fetch_category_to_scrape(db_connection, 'First Supermarket')
    assert expected_result == result


def test_fetch_category_to_scrape_fetches(db_connection):
    """
    test that fetch_category_to_scrape fetches a category with
    last_scraped_on with scraped_on more than 7 days from now in the past
    """
    expected_result = Category(
        supermarket_id=2,
        category_id='test_id_3',
        name='Third Category',
        last_scraped_on=ten_days_ago
    )
    result = fetch_category_to_scrape(db_connection, 'Second Supermarket')
    assert expected_result == result


def test_fetch_products_ids(db_connection):
    """
    test that fetch_products_ids fetches the expected list of product ids for
    a given category
    """
    expected_result = ['product_id_1', 'product_id_2']
    fetched_results = fetch_products_ids(db_connection, 1, 'test_id_1')
    assert fetched_results == expected_result


@pytest.fixture
def custom_product():
    """creates a custom product with predefined parameters"""
    product_info = ProductInfo(
        product_id='X45',
        observed_on=datetime.date(year=1970, month=1, day=3),
        price=Decimal('149.99'),
        discounted_price=None,
        rating=Decimal('4.60'),
        rates_count=520,
        unit='1 kg'
    )
    product = Product(
        product_id='X45',
        supermarket_id=1,
        category_id='BM12',
        name='Apples',
        created_on=datetime.date(year=1970, month=1, day=1),
        product_info=product_info
    )
    return product
