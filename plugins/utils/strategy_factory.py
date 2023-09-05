import json
import logging
import os
import importlib


class StrategyFactory:
    def __init__(self):
        self.mapping = self.get_mapping_file()
        if not self.mapping:
            raise ValueError(f'No mapping found')

    def get_strategy(self, task_type: str, name: str):
        return self._load_strategy(task_type, name)

    def get_strategy_name(self, website: str):
        for scraper in self.mapping['scrapers']:
            if scraper['website'] == website:
                strategy_name = scraper['name']
                return strategy_name

    @staticmethod
    def get_mapping_file():
        current_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.join(current_dir, '..')
        mapping_path = os.path.join(root_dir, 'mapping.json')

        with open(mapping_path, 'r') as file:
            mapping = json.load(file)

        return mapping

    @staticmethod
    def _load_strategy(module, strategy_name):
        sub_module = importlib.import_module(f'strategies.{module}.{strategy_name.lower()}')

        class_name = ''.join(word.title() for word in strategy_name.split('_'))
        class_name = f'{class_name}{module.title()}Strategy'
        strategy_class = getattr(sub_module, class_name)

        logging.info(f'Loaded strategy: {class_name}')
        return strategy_class()
