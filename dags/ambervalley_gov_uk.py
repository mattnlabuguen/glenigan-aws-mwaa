import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from scripts.utils.strategy_utils import get_parsing_strategy, get_crawling_strategy
from scripts.file_handler.file_pickler import FilePickler
from scripts.file_handler.csv_writer import CsvWriter

default_args = {
    'owner': 'BCI Central'
}
website_name = 'ambervalley.gov.uk'
months_ago = 6

crawler = get_crawling_strategy(website_name=website_name)
parser = get_parsing_strategy(website_name=website_name)
file_handler = FilePickler()
writer = CsvWriter()
date_today = datetime.now().strftime('%Y-%m-%d')


@dag(default_args=default_args, start_date=days_ago(2), tags=['glenigan'])
def ambervalley_gov_uk():
    @task()
    def get_sources():
        application_sources = crawler.get_sources(months_ago=months_ago)
        return application_sources

    @task()
    def crawl(reference_number: str) -> list:
        raw_data = crawler.crawl(ref_val=reference_number)
        return raw_data

    @task()
    def parse(raw_data: str) -> list:
        processed_data = parser.parse(raw_data)
        return processed_data

    @task()
    def dump_raw_data(raw_data: list) -> list:
        file_name = f"{website_name.replace('.', '_')}_raw_data_{date_today}"
        file_handler.dump(raw_data, file_name)

    @task()
    def write_to_csv(data: list) -> list:
        file_name = f"{website_name.replace('.', '_')}_parsed_data_{date_today}"
        writer.write(data, file_name)

    sources = get_sources()
    raw_data_list = crawl.expand(reference_number=sources)
    dump_raw_data(raw_data_list)
    parsed_data = parse.expand(raw_data=raw_data_list)
    write_to_csv(parsed_data)


ambervalley_dag = ambervalley_gov_uk()
