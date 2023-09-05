from abc import ABC, abstractmethod


class ParseStrategy(ABC):
    @abstractmethod
    def parse(self, **kwargs):
        pass
