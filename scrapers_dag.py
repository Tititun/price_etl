"""
this module defines DAGs for each scraper located in scrapers module
"""
import datetime

from airflow.decorators import dag, task

from scrapers.common import telegram_callback_on_failure

for i, scraper_module in enumerate(['pyaterochka', 'lenta', 'magnit']):
    @dag(
        dag_id=f'{scraper_module}_scraper',
        schedule=f'{i}/5 * * * *',
        start_date=datetime.datetime(2024, 1, 1),
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
            'environment': {'scraper': '{{ params.scraper }}',
                            'run_id': '{{ run_id }}'},
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

        @task.docker(skip_on_exit_code=10, **docker_task_args)
        def fetch_data(category: Category) -> RequestData:
            """
            A task that requests data for the category from the supermarket's
            website
            """
            import sys
            if not category:
                # mark task as skipped if there are no category to scrape
                sys.exit(10)
            import importlib
            import os
            scraper = os.environ['scraper']
            s_module = importlib.import_module(f'scrapers.{scraper}.scraper')
            request_data = s_module.request_data
            return request_data(category)

        @task.branch()
        def is_data_empty(ti) -> str:
            """
            checks if the data returned by the fetch_data task is empty
            if it's empty, triggers update_category_last_empty_on task,
            if not - triggers transform task
            """
            data = ti.xcom_pull(key='return_value', task_ids='fetch_data')
            if not data.data['products']:
                return 'update_last_empty_on'
            else:
                return 'transform'

        @task.docker(**docker_task_args)
        def transform() -> ProductList:
            """
            A task that  takes raw json data received from the supermarket's
            website and transforms it into a ProductList
            """
            import importlib
            import os
            from db.mysql_functions import airflow_category, airflow_data

            scraper = os.environ['scraper']
            s_module = importlib.import_module(f'scrapers.{scraper}.scraper')

            category = airflow_category()
            data = airflow_data()
            product_list = s_module.parse_data(data, category)

            return product_list

        @task.docker(**docker_task_args)
        def upsert() -> datetime.date:
            """
            A task that inserts new products and product_infos, updates
            existing product's information
            """
            from db.mysql_functions import (airflow_category,
                                            airflow_product_list, mysql_connect,
                                            upsert_product_list)

            category = airflow_category()
            product_list = airflow_product_list()
            with mysql_connect() as conn:
                return upsert_product_list(conn, product_list, category)

        @task.docker(**docker_task_args)
        def update_last_empty_on():
            """
            A task that updates the scraped category's last_empty_on field
            """
            from db.mysql_functions import (airflow_data,
                                            update_category_date_field,
                                            mysql_connect)

            request_data = airflow_data()
            date = request_data.date
            category = request_data.category
            with mysql_connect() as conn:
                update_category_date_field(conn, category, date,
                                           'last_empty_on')

        @task.docker(**docker_task_args)
        def update_last_scraped_on(date: datetime.date):
            """
            A task that updates the scraped category's last_update_on field
            """
            from db.mysql_functions import (airflow_category,
                                            update_category_date_field,
                                            mysql_connect)

            category = airflow_category()
            with mysql_connect() as conn:
                update_category_date_field(conn, category, date,
                                           'last_scraped_on')

        category_to_scrape = fetch_category()
        scraped_data = fetch_data(category_to_scrape)
        transformed_data = transform()
        scraped_data >> is_data_empty() >> [transformed_data,
                                            update_last_empty_on()]
        date_ = upsert()
        transformed_data >> date_
        update_last_scraped_on(date_)


    supermarket_dag()
