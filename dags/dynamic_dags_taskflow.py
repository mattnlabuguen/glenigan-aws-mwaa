import io
import json
import logging
from datetime import datetime
from typing import List, Dict

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from utils.strategy_factory import StrategyFactory

DEFAULT_OWNER = 'BCI Central'
DEFAULT_MONTHS_AGO = 1


def create_dag(dag_id, schedule, default_args, source_strategy, crawl_strategy, parse_strategy):
    """
    Creates a dynamically generated DAG based on the parameters.
    :param dag_id: The DAG ID
    :param schedule: The DAG schedule in cron format as a string
    :param default_args: The DAG default args
    :param source_strategy: The source strategy
    :param crawl_strategy: The crawler strategy
    :param parse_strategy: The parser strategy
    :return:
    """
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        start_date=days_ago(2),
        catchup=False,
        tags=['glenigan']
    )
    def dynamically_generated_dag():
        @task()
        def get_application_sources(months_ago: int = DEFAULT_MONTHS_AGO, **context) -> List[str]:
            """
            Gets the application sources from the source strategy.
            :param months_ago: The number of months ago to start getting sources from.
            :param context: The context of the task.
            :return: A list of application sources.
            """
            dag_run = context['dag_run']

            # If the months_ago is in the dag_run.conf, use that instead of the default.
            if 'months_ago' in dag_run.conf:
                months_ago = dag_run.conf['months_ago']
                logging.info(f'Getting months_ago from dag_run.conf instead of default: {months_ago}')

            # If the sources are in the dag_run.conf, use that instead of the default unless it is None or empty.
            if 'sources' in dag_run.conf and (dag_run.conf['sources'] is not None and dag_run.conf['sources'] != []):
                logging.info(f'Getting sources from dag_run.conf instead of default: {dag_run.conf["sources"]}')
                return dag_run.conf['sources']

            logging.info(f'Getting sources from {source_strategy.__class__.__name__}')
            return source_strategy.get_sources(months_ago=months_ago)

        @task()
        def crawl_web_data(source: str) -> Dict:
            """
            Crawls the web data from the source.
            :param source: The source or URL to crawl.
            :return: The raw data from the source in a dictionary.
            """
            return crawl_strategy.crawl(source)

        @task()
        def parse_raw_data(raw_data: str) -> Dict:
            """
            Parses the raw data into a dictionary.
            :param raw_data: The raw data to parse.
            :return: The parsed data in a dictionary.
            """
            return parse_strategy.parse(raw_data)

        @task()
        def upload_to_s3(data: List, type: str, **context):
            """
            Uploads the data to S3.
            :param data: The data to upload.
            :param type: The type of data to upload, can be either 'raw' or 'parsed'.
            :param context: The context of the task.
            """
            s3_hook = S3Hook(aws_conn_id='aws_default')
            bucket_name = 'bci-mwaa'
            key = f'output/{dag_id}/{type}/{context["run_id"]}_{context["ds"]}_{type}.json'
            data = json.dumps(data, indent=4, sort_keys=True, default=str)
            s3_hook.load_string(data, key, bucket_name)

        application_sources = get_application_sources(months_ago=DEFAULT_MONTHS_AGO)

        raw_data = crawl_web_data.expand(source=application_sources)
        upload_to_s3(data=raw_data, type='raw')

        parsed_data = parse_raw_data.expand(raw_data=raw_data)
        upload_to_s3(data=parsed_data, type='parsed')

    generated_dag = dynamically_generated_dag()

    return generated_dag


strategy_factory = StrategyFactory()
mapping = strategy_factory.get_mapping_file()
default_args = {'owner': DEFAULT_OWNER}

for scraper in mapping['scrapers']:
    name = scraper['name']
    schedule = scraper['schedule']
    default_args = default_args

    source = strategy_factory.get_strategy('source', name)
    crawler = strategy_factory.get_strategy('crawl', name)
    parser = strategy_factory.get_strategy('parse', name)

    if all([source, crawler, parser]):
        create_dag(
            dag_id=name,
            schedule=schedule,
            default_args=default_args,
            source_strategy=source,
            crawl_strategy=crawler,
            parse_strategy=parser
        )
