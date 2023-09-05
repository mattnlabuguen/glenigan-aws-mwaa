from abc import ABC, abstractmethod


class SourceStrategy(ABC):
    @abstractmethod
    def get_sources(self, **kwargs):
        pass
