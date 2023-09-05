from abc import ABC, abstractmethod


class CrawlStrategy(ABC):
    @abstractmethod
    def crawl(self, **kwargs):
        pass
