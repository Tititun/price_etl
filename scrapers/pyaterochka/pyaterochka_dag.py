"""
this module defines an airflow DAG for pyaterochka ETL pipeline
"""
import datetime

from airflow.decorators import dag, task

@dag(
    schedule=None,
    start_date=None,
    catchup=False,
    tags=['scrapers']
)
def pyaterochka_scraper():
    """
    this is a data pipeline which scrapes products' data from 5ka.ru
    supermarket
    """
    from airflow.models import TaskInstance
    from scrapers.common import Category, ProductList, RequestData

    @task()
    def fetch_category(ti: TaskInstance) -> Category:
        """
        A task to fetch a random category that hasn't been scraped
        or was scraped more than 6 days ago
        """
        from scrapers.pyaterochka.common import SUPERMARKET_NAME
        from db.mysql_functions import (fetch_supermarket_by_name,
                                        fetch_category_to_scrape,
                                        mysql_connect)
        with mysql_connect() as conn:
            supermarket = fetch_supermarket_by_name(conn, SUPERMARKET_NAME)
            category = fetch_category_to_scrape(conn, supermarket)
        ti.xcom_push(key='category', value=category)
        return category

    @task()
    def fetch_data(category: Category) -> RequestData:
        """
        A task that requests data for the category from the server
        """
        from scrapers.pyaterochka.scraper import request_data
        return request_data(category)

    @task()
    def transform_data(data: ProductList, ti: TaskInstance) -> ProductList:
        """
        A task that  takes raw json data and transforms it into a ProductList
        """
        from scrapers.pyaterochka.scraper import parse_data
        category = ti.xcom_pull(task_ids='fetch_category', key='category')
        return parse_data(data, category)

    @task()
    def upsert(product_list: ProductList, ti: TaskInstance) -> datetime.date:
        """
        A task that inserts new products and product_infos, updates
        existing product's information
        """
        from db.mysql_functions import mysql_connect, upsert_product_list
        category = ti.xcom_pull(task_ids='fetch_category', key='category')
        with mysql_connect() as conn:
            return upsert_product_list(conn, product_list, category)

    @task()
    def update_category(date: datetime.date, ti: TaskInstance):
        """
        A task that updates the scraped category's last_update_on field
        """
        from db.mysql_functions import (update_category_last_scraped_on,
                                        mysql_connect)
        category = ti.xcom_pull(task_ids='fetch_category', key='category')
        with mysql_connect() as conn:
            update_category_last_scraped_on(conn, category, date)

    category_to_scrape = fetch_category()
    scraped_data = fetch_data(category_to_scrape)
    parsed_data = transform_data(scraped_data)
    observed_date = upsert(parsed_data)
    update_category(observed_date)


pyaterochka_scraper()
