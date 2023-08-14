from abc import ABC, abstractmethod


class ParsingStrategy(ABC):
    @abstractmethod
    def parse(self, raw_data):
        pass
