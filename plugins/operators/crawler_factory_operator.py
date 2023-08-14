from airflow.models.baseoperator import BaseOperator

from plugins.strategies.utils.strategy_utils import get_crawling_strategy


class CrawlerFactoryOperator(BaseOperator):
    def __init__(self, website_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.website_name = website_name

    def execute(self, **kwargs):
        crawling_strategy = get_crawling_strategy(self.website_name)

        return crawling_strategy


c = CrawlerFactoryOperator(
    task_id='test_task',
    website_name='planning.wandsworth.gov.uk'
)
strategy = c.execute()
