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

from db.mysql_functions import (fetch_products_codes,
                                fetch_product_codes_map,
                                fetch_category_to_scrape,
                                fetch_supermarket_categories,
                                fetch_supermarket_by_id,
                                fetch_supermarket_by_name,
                                insert_new_products,
                                insert_product_infos,
                                upsert_categories,
                                upsert_product_list,
                                update_category_last_scraped_on,
                                update_existent_products)
from scrapers.common import (Category, Product, ProductInfo, ProductList,
                             Supermarket, get_today_date)

load_dotenv()
today = get_today_date()
ten_days_ago = today - datetime.timedelta(days=10)

# SQL queries to execute during the setup in db_connection fixture
setup_queries = [
    """
        INSERT INTO supermarkets VALUES
        (1, "First Supermarket"), (2, "Second Supermarket");
    """,

    f"""
        INSERT INTO categories
        (category_id, supermarket_id, category_code, name, last_scraped_on)
        VALUES
        (1, 1, "test_id_1", "First Category", null),
        (2, 1, "test_id_2", "Second Category", "{today}"),
        (3, 2, "test_id_3", "Third Category", "{ten_days_ago}");
    """,
    
    """
        INSERT INTO products
        (product_id, product_code, category_id, name, created_on)
        VALUES
        (1, "product_id_1", 1, "Product 1", "2020-01-01"),
        (2, "product_id_2", 1, "Product 2", "2020-01-02"),
        (3, "product_id_3", 2, "Product 3", "2020-01-03");
    """,

    """
        INSERT INTO product_info
        (product_id, observed_on, price, discounted_price,
         rating, rates_count, unit)
        VALUES
        (1, '2020-01-01', 100.00, 90.00, 4.4, 5, '1 kg'),
        (2, '2020-01-02', 200.00, null, 5.0, 12, '1 l')
    """
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


def test_fetch_supermarket_by_name_fetches(db_connection):
    """
    test if fetch_supermarket_by_name returns correct Supermarket
    """
    name = 'First Supermarket'
    result = fetch_supermarket_by_name(db_connection, name)
    assert result == Supermarket(supermarket_id=1, name=name)


def test_fetch_supermarket_by_name_fetches_none(db_connection):
    """
    test if fetch_supermarket_by_name returns None
    for a non-existent supermarket
    """
    result = fetch_supermarket_by_name(db_connection, 'Imaginary Supermarket')
    assert result is None


def test_fetch_supermarket_by_id(db_connection):
    """
    test if fetch_supermarket_by_id returns name for an existent supermarket
    """
    result = fetch_supermarket_by_id(db_connection, 1)
    assert result == Supermarket(supermarket_id=1, name='First Supermarket')


def test_fetch_supermarket_categories(db_connection):
    """
    test if fetch_supermarket_categories returns a list of expected Category
    objects
    """
    supermarket = Supermarket(supermarket_id=1, name='First Supermarket')
    result = fetch_supermarket_categories(db_connection, supermarket)
    expected = [
        Category(supermarket_id=1, category_id=1, category_code='test_id_1',
                 name='First Category'),
        Category(supermarket_id=1, category_id=2, category_code='test_id_2',
                 name='Second Category', last_scraped_on=today),
    ]
    assert result == expected


def test_upsert_categories_new_categories(db_connection):
    """
    test if new categories are inserted successfully
    """
    categories_to_insert = [
        Category(
            supermarket_id=2, category_id=None, category_code='test_id_4',
            name='Fourth Category'),
        Category(
            supermarket_id=2, category_id=None, category_code='test_id_5',
            name='Fifth Category'),
    ]
    expected_result = [
        Category(
            supermarket_id=2, category_id=3, category_code='test_id_3',
            name='Third Category', last_scraped_on=ten_days_ago),
        Category(
            supermarket_id=2, category_id=4, category_code='test_id_4',
            name='Fourth Category'),
        Category(
            supermarket_id=2, category_id=5, category_code='test_id_5',
            name='Fifth Category'),
    ]
    supermarket = Supermarket(supermarket_id=2, name="Second Supermarket")
    upsert_categories(db_connection, categories_to_insert)
    categories = fetch_supermarket_categories(db_connection, supermarket)
    assert categories == expected_result


def test_upsert_categories_no_duplicates(db_connection):
    """
    test that categories with duplicate category_id are not inserted
    """
    categories = [
        Category(
            supermarket_id=2, category_id=3, category_code='test_id_3',
            name='Third Category Different', last_scraped_on=ten_days_ago),
        Category(
            supermarket_id=2, category_id=4, category_code='test_id_4',
            name='Fourth Category'),
    ]
    supermarket = Supermarket(supermarket_id=2, name="Second Supermarket")
    upsert_categories(db_connection, categories)
    categories = fetch_supermarket_categories(db_connection, supermarket)
    assert categories == categories


def test_fetch_category_to_scrape_none(db_connection):
    """
    test that fetch_category_to_scrape fetches a category with
    last_scraped_on = None
    """
    expected_result = Category(
        category_id=1,
        supermarket_id=1,
        category_code='test_id_1',
        name='First Category',
        last_scraped_on=None
    )
    supermarket = Supermarket(supermarket_id=1, name='First Supermarket')
    result = fetch_category_to_scrape(db_connection, supermarket)
    assert expected_result == result


def test_fetch_category_to_scrape_fetches(db_connection):
    """
    test that fetch_category_to_scrape fetches a category with
    last_scraped_on with scraped_on more than 7 days from now in the past
    """
    expected_result = Category(
        category_id=3,
        supermarket_id=2,
        category_code='test_id_3',
        name='Third Category',
        last_scraped_on=ten_days_ago
    )
    supermarket = Supermarket(supermarket_id=2, name='Second Supermarket')
    result = fetch_category_to_scrape(db_connection, supermarket)
    assert expected_result == result


def test_fetch_products_codes(db_connection):
    """
    test that fetch_products_ids fetches the expected list of product ids for
    a given category
    """
    expected_result = ['product_id_1', 'product_id_2']
    category = Category(supermarket_id=1, category_id=1,
                        category_code='test_id_1', name='First Category')
    fetched_results = fetch_products_codes(db_connection, category)
    assert fetched_results == expected_result


@pytest.fixture
def starter_products() -> ProductList:
    """
    creates a list of products that are inserted into test_database
    upon its creation
    """
    return ProductList(
        items=[
            Product(
                product_id=1,
                product_code='product_id_1',
                supermarket_id=1,
                category_id=1,
                name='Product 1',
                created_on=datetime.date(year=2020, month=1, day=1),
            ),
            Product(
                product_id=2,
                product_code='product_id_2',
                supermarket_id=1,
                category_id=1,
                name='Product 2',
                created_on=datetime.date(year=2020, month=1, day=2),
            )
        ]
    )


def test_update_existent_products(db_connection, starter_products):
    """
    test that update_existent_products updates names of products
    """
    starter_products.items[0].name = 'Product X'
    update_existent_products(db_connection, starter_products)
    with db_connection.cursor() as cursor:
        cursor.execute("""
                        SELECT product_id, name
                        FROM products
                        WHERE product_id <= 2
                        ORDER BY product_id
                       """)
        result = cursor.fetchall()
    assert result == [(1, 'Product X'),
                      (2, 'Product 2')]


@pytest.fixture
def new_products() -> ProductList:
    """
    creates a list of new products that test_database doesn't have in its
    products table
    """
    return ProductList(
        items=[
            Product(
                product_id=None,
                product_code='product_id_4',
                supermarket_id=1,
                category_id=2,
                name='Product 4',
                created_on=datetime.date(year=2020, month=1, day=5),
            ),
            Product(
                product_id=None,
                product_code='product_id_5',
                supermarket_id=1,
                category_id=2,
                name='Product 5',
                created_on=datetime.date(year=2020, month=1, day=5),
            )
        ]
    )


def test_insert_new_products(db_connection, new_products):
    """
    test that insert_new_products insets products from provided ProductList
    """
    insert_new_products(db_connection, new_products)
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
                SELECT
                    p.product_id, p.product_code, p.category_id,
                    p.name, p.created_on
                FROM products p JOIN categories c USING (category_id)
                WHERE c.supermarket_id=1 AND p.category_id=2;
            """)
        result = cursor.fetchall()

    expected_result = [
        (3, 'product_id_3', 2, 'Product 3', datetime.date(2020, 1, 3)),
        (4, 'product_id_4', 2, 'Product 4', datetime.date(2020, 1, 5)),
        (5, 'product_id_5', 2, 'Product 5', datetime.date(2020, 1, 5)),
    ]
    assert result == expected_result


@pytest.fixture
def new_product_infos():
    return [
       ProductInfo(
           product_id=1,
           observed_on=datetime.date(year=2020, month=1, day=10),
           price=Decimal('149.99'),
           discounted_price=None,
           rating=Decimal('4.60'),
           rates_count=520,
           unit='1 kg'
       ),
       ProductInfo(
           product_id=2,
           observed_on=datetime.date(year=2020, month=1, day=10),
           price=Decimal('199.99'),
           discounted_price=Decimal('189.99'),
           rating=Decimal('4.90'),
           rates_count=800,
           unit='2 l'
       )
    ]


def test_insert_product_infos(db_connection, new_product_infos):
    """
    test that new records are inserted into product_info table as expected
    """
    insert_product_infos(db_connection, new_product_infos)
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
                SELECT
                    pi.product_id, observed_on, price,
                    discounted_price, rating, rates_count, unit
                FROM
                    product_info pi JOIN products p USING (product_id)
                    JOIN categories c USING (category_id)
                WHERE c.supermarket_id=1 AND pi.observed_on='2020-01-10'
                ORDER BY product_id
            """)
        result = cursor.fetchall()
    expected_result = [
        (1, datetime.date(year=2020, month=1, day=10),
         Decimal('149.99'), None, Decimal('4.60'), 520, '1 kg'),
        (2, datetime.date(year=2020, month=1, day=10),
         Decimal('199.99'), Decimal('189.99'), Decimal('4.90'), 800, '2 l'),
    ]
    assert result == expected_result


def test_fetch_product_ids(db_connection, starter_products):
    """
    test that fetch_product_codes_map return correct mapping of
    product codes to product ids
    """
    for product in starter_products.items:
        product.product_id = None

    expected_result = {
        'product_id_1': 1,
        'product_id_2': 2
    }
    result = fetch_product_codes_map(db_connection, starter_products)
    assert expected_result == result


@pytest.fixture
def upsert_list():
    items = [
        Product(
            product_id=None,
            product_code='product_id_1',
            supermarket_id=1,
            category_id=1,
            name='New Product 1 Name',   # new name
            created_on=datetime.date(year=2021, month=1, day=1),  # changed date
            product_info=ProductInfo(
                product_id=None,
                observed_on=datetime.date(year=2021, month=1, day=1),
                price=Decimal('210.99'),
                discounted_price=Decimal('189.99'),
                rating=Decimal('4.95'),
                rates_count=800,
                unit='1 kg'
            )
        ),
        Product(
            product_id=None,
            product_code='product_id_2',
            supermarket_id=1,
            category_id=1,
            name='Product 2',
            created_on=datetime.date(year=2021, month=1, day=1),  # changed date
            product_info=ProductInfo(
                product_id=None,
                observed_on=datetime.date(year=2021, month=1, day=1),
                price=Decimal('150.00'),
                discounted_price=None,
                rating=Decimal('4.99'),
                rates_count=1001,
                unit='1 l'
            )
        ),
        # new product:
        Product(
            product_id=None,
            product_code='product_id_4',
            supermarket_id=1,
            category_id=1,
            name='Product 4',
            created_on=datetime.date(year=2021, month=1, day=1),
            product_info=ProductInfo(
                product_id=None,
                observed_on=datetime.date(year=2021, month=1, day=1),
                price=Decimal('500.00'),
                discounted_price=None,
                rating=None,
                rates_count=0,
                unit='50g'
            )
        ),
    ]
    return ProductList(items=items)


@pytest.fixture
def category():
    """returns the first Category from test_database"""
    return Category(
        category_id=1,
        supermarket_id=1,
        category_code='test_id_1',
        name='First Category'
    )


def test_upsert_product_list(db_connection, upsert_list, category):
    """
    test that upsert_product_list upserts the upsert_list correctly
    which includes:
     -inserting new product
     -updating existent products' info
     -inserting new price infos
    """
    upsert_product_list(db_connection, upsert_list, category)
    with db_connection.cursor() as cursor:
        cursor.execute(
            """
               SELECT
                 p.product_id, p.product_code, p.category_id, p.name,
                 p.created_on, pi.observed_on, pi.price, pi.discounted_price,
                 pi.rating, pi.rates_count, pi.unit
               FROM products p JOIN product_info pi USING (product_id)
               WHERE p.category_id=1
               ORDER BY p.product_id, pi.observed_on
            """
        )
        result = cursor.fetchall()
    expected_result = [
        (1, 'product_id_1', 1, 'New Product 1 Name', datetime.date(2020, 1, 1),
         datetime.date(2020, 1, 1), Decimal('100.00'), Decimal('90.00'),
         Decimal('4.40'), 5, '1 kg'),
        (1, 'product_id_1', 1, 'New Product 1 Name', datetime.date(2020, 1, 1),
         datetime.date(2021, 1, 1), Decimal('210.99'), Decimal('189.99'),
         Decimal('4.95'), 800, '1 kg'),

        (2, 'product_id_2', 1, 'Product 2', datetime.date(2020, 1, 2),
         datetime.date(2020, 1, 2), Decimal('200.00'), None,
         Decimal('5.00'), 12, '1 l'),
        (2, 'product_id_2', 1, 'Product 2', datetime.date(2020, 1, 2),
         datetime.date(2021, 1, 1), Decimal('150.00'), None,
         Decimal('4.99'), 1001, '1 l'),

        (4, 'product_id_4', 1, 'Product 4', datetime.date(2021, 1, 1),
         datetime.date(2021, 1, 1), Decimal('500.00'), None,
         None, 0, '50g'),
    ]
    assert result == expected_result


def test_upsert_product_list_returns_date(db_connection, upsert_list, category):
    """
    test that upsert_product_list returns a date of upserted products
    """
    returned_date = upsert_product_list(db_connection, upsert_list, category)
    expected_date = datetime.date(year=2021, month=1, day=1)
    assert returned_date == expected_date


def test_update_category_last_scraped_on(db_connection, category):
    """
    test that update_category_last_scraped_on updates the last_scraped_on
    of the category in the database
    """
    date = datetime.date(2021, 5, 5)
    update_category_last_scraped_on(db_connection, category, date)
    with db_connection.cursor() as cursor:
        cursor.execute("""
                       SELECT last_scraped_on
                       FROM categories 
                       WHERE category_id=%s
                       """, (category.category_id,))
        last_scraped_on = cursor.fetchone()[0]
    assert last_scraped_on == date


def test_update_category_last_scraped_on_fails(db_connection, category):
    """
    test that update_category_last_scraped_on raises ValueError when we try
    to set a date from the future
    """
    date = datetime.date(2100, 5, 5)
    with pytest.raises(ValueError):
        update_category_last_scraped_on(db_connection, category, date)
