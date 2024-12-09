"""
this module contains tests that cover database operations
"""
import os
from pathlib import Path

from dotenv import load_dotenv
import mysql.connector
import pytest

from db.mysql_functions import (fetch_supermarket_categories,
                                fetch_supermarket_id,
                                fetch_supermarket_name,
                                upsert_categories)
from scrapers.common import Category

load_dotenv()

# SQL queries to execute during the setup in db_connection fixture
setup_queries = [
    'INSERT INTO supermarkets VALUES (1, "First Supermarket"),'
    ' (2, "Second Supermarket");',

    'INSERT INTO categories (supermarket_id, category_id, name) '
    'VALUES '
    '(1, "test_id_1", "First Category"),'
    '(1, "test_id_2", "Second Category"),'
    '(2, "test_id_3", "Third Category");'
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
                 name='Second Category'),
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
            name='Third Category'),
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
            name='Third Category Different'),
        Category(
            supermarket_id=None, category_id='test_id_4',
            name='Fourth Category'),
    ]
    expected_result = [
        Category(
            supermarket_id=2, category_id='test_id_3',
            name='Third Category'),
        Category(
            supermarket_id=2, category_id='test_id_4',
            name='Fourth Category'),
    ]
    supermarket = "Second Supermarket"
    upsert_categories(db_connection, categories_to_insert,
                      supermarket=supermarket)
    categories = fetch_supermarket_categories(db_connection, supermarket)
    assert categories == expected_result
