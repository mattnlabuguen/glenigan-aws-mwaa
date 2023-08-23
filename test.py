from plugins.scripts.utils.strategy_utils import get_parsing_strategy, get_crawling_strategy

if __name__ == '__main__':
    website_name = 'ambervalley.gov.uk'

    crawler = get_crawling_strategy(website_name=website_name)
    parser = get_parsing_strategy(website_name=website_name)

    application_sources = crawler.get_sources(months_ago=1)
    raw_data = crawler.crawl(reference_numbers=application_sources)
    parsed_data = parser.parse(raw_data)