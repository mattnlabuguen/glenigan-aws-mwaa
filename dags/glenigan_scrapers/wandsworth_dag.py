import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from scripts.strategies.utils.strategy_utils import get_parsing_strategy, get_crawling_strategy
from scripts.strategies.file_handler.file_pickler import FilePickler
from scripts.strategies.file_handler.csv_writer import CsvWriter

default_args = {
    'owner': 'BCI Central'
}
website_name = 'planning.wandsworth.gov.uk'
max_pages = 1

crawler = get_crawling_strategy(website_name=website_name)
parser = get_parsing_strategy(website_name=website_name)
file_handler = FilePickler()
writer = CsvWriter()
date_today = datetime.now().strftime('%Y-%m-%dT%H%M%S')

@dag(default_args=default_args, start_date=days_ago(2), tags=['glenigan'])
def wandsworth():
    @task()
    def get_sources():
        application_urls = crawler.get_sources(max_pages=max_pages)
        return application_urls

    @task()
    def crawl(application_urls: list) -> list:
        raw_data = crawler.crawl(application_urls=application_urls)
        return raw_data

    @task()
    def parse(raw_data: list) -> list:
        processed_data = []
        for data in raw_data:
            logging.info(str(data))
            parsed_data = parser.parse(data)
            processed_data.append(parsed_data)

        return processed_data

    @task()
    def dump_raw_data(raw_data: list) -> list:
        file_name = f'raw_wandsworth_{date_today}'
        file_handler.dump(raw_data, file_name)

    @task()
    def write_to_csv(parsed_data: list) -> list:
        file_name = f'final_wandsworth_{date_today}'
        writer.write(parsed_data, file_name)

    sources = get_sources()
    raw_data_list = crawl(sources)
    dump_raw_data(raw_data_list)
    parsed_data_list = parse(raw_data_list)
    write_to_csv(parsed_data_list)

wandsworth_taskflow = wandsworth()
