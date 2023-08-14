from abc import abstractmethod, ABC


class CrawlingStrategy(ABC):
    @abstractmethod
    def download(self, url, timeout=10, headers=None, cookies=None, data=None):
        pass

    @abstractmethod
    def crawl(self):
        pass
