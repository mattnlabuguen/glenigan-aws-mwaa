from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from glenigan_aws_mwaa.scripts.strategies.utils.strategy_utils import get_parsing_strategy, get_crawling_strategy

default_args = {
    'owner': 'BCI Central'
}
website_name = 'wandsworth.gov.uk'
max_pages = 240


@dag(default_args=default_args, start_date=days_ago(2), tags=['glenigan'])
def wandsworth_taskflow():
    @task()
    def crawl() -> list:
        crawler = get_crawling_strategy(website_name=website_name)
        raw_data = crawler.crawl(max_pages=max_pages)

        return raw_data

    @task()
    def parse(raw_data: list) -> list:
        parser = get_parsing_strategy(website_name=website_name)
        parsed_data = parser.parse(raw_data)

        return parsed_data

    raw_data_list = crawl()
    parsed_data_list = parse(raw_data_list)


wandsworth_taskflow = wandsworth_taskflow()
