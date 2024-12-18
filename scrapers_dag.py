"""
this module defines DAGs for each scraper located in scrapers module
"""
import datetime

from airflow.decorators import dag, task

from scrapers.common import telegram_callback_on_failure


for i, scraper_module in enumerate(['pyaterochka']):
    @dag(
        dag_id=f'{scraper_module}_scraper',
        schedule=f'{i}/5 * * * *',
        start_date=datetime.datetime(2024,1,1),
        catchup=False,
        on_failure_callback=telegram_callback_on_failure,
        tags=['scrapers'],
        params={'scraper': scraper_module}
    )
    def supermarket_dag():
        """
        this is a data pipeline which scrapes products, cleans the data and
        pushes it to the database
        """
        from scrapers.common import Category, ProductList, RequestData
        docker_task_args = {
            'auto_remove': 'force',
            'environment': {'scraper': '{{ params.scraper }}'},
            'image': 'price_etl',
            'network_mode': 'host'
        }

        @task.docker(**docker_task_args)
        def fetch_category() -> Category:
            """
            A task to fetch a random category that hasn't been scraped
            or was scraped more than 6 days ago
            """
            import importlib
            import os
            scraper = os.environ['scraper']
            common = importlib.import_module(f'scrapers.{scraper}.common')
            from db.mysql_functions import (fetch_supermarket_by_name,
                                            fetch_category_to_scrape,
                                            mysql_connect)

            supermarket_name = common.SUPERMARKET_NAME
            with mysql_connect() as conn:
                supermarket = fetch_supermarket_by_name(conn, supermarket_name)
                category = fetch_category_to_scrape(conn, supermarket)
            return category

        @task.docker(**docker_task_args)
        def fetch_data(category: Category) -> RequestData:
            """
            A task that requests data for the category from the supermarket's
            website
            """
            import importlib
            import os
            scraper = os.environ['scraper']
            s_module = importlib.import_module(f'scrapers.{scraper}.scraper')
            request_data = s_module.request_data
            return request_data(category)

        @task.docker(**docker_task_args)
        def transform(data: RequestData, category: Category) -> ProductList:
            """
            A task that  takes raw json data received from the supermarket's
            website and transforms it into a ProductList
            """
            import importlib
            import os
            scraper = os.environ['scraper']
            s_module = importlib.import_module(f'scrapers.{scraper}.scraper')
            product_list = s_module.parse_data(data, category)
            if not product_list.items:
                raise IndexError
            return product_list

        @task.docker(**docker_task_args)
        def upsert(product_list: ProductList, category: Category)\
                -> datetime.date:
            """
            A task that inserts new products and product_infos, updates
            existing product's information
            """
            from db.mysql_functions import mysql_connect, upsert_product_list
            with mysql_connect() as conn:
                return upsert_product_list(conn, product_list, category)

        @task.docker(**docker_task_args)
        def update_category(date: datetime.date, category: Category):
            """
            A task that updates the scraped category's last_update_on field
            """
            from db.mysql_functions import (update_category_last_scraped_on,
                                            mysql_connect)
            with mysql_connect() as conn:
                update_category_last_scraped_on(conn, category, date)

        category_to_scrape = fetch_category()
        scraped_data = fetch_data(category_to_scrape)
        parsed_data = transform(scraped_data, category_to_scrape)
        observed_date = upsert(parsed_data, category_to_scrape)
        update_category(observed_date, category_to_scrape)


    supermarket_dag()
