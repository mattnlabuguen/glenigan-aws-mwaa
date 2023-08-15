import importlib
import json
import os

RELATIVE_MAPPING_PATH = "/scripts/strategies/mapping.json"


def get_crawling_strategy(website_name: str):
    with open(RELATIVE_MAPPING_PATH, "r") as file:
        mapping = json.load(file)
        file_name = mapping[website_name]

    try:
        module = importlib.import_module(f'plugins.strategies.crawler.{file_name}')
        class_name = f"{''.join([element.capitalize() for element in file_name.split('_')])}CrawlingStrategy"
        crawling_strategy = getattr(module, class_name)

    except AttributeError:
        crawling_strategy = None

    return crawling_strategy()


def get_parsing_strategy(website_name: str):
    with open(RELATIVE_MAPPING_PATH, "r") as file:
        mapping = json.load(file)
        file_name = mapping[website_name]

    try:
        module = importlib.import_module(f'plugins.strategies.parser.{file_name}')
        class_name = f"{''.join([element.capitalize() for element in file_name.split('_')])}ParsingStrategy"
        parsing_strategy = getattr(module, class_name)

    except AttributeError:
        parsing_strategy = None

    return parsing_strategy()
