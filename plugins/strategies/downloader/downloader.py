from abc import abstractmethod, ABC


class DownloaderStrategy(ABC):
    @abstractmethod
    def get(self, url, timeout=10, headers=None, cookies=None):
        pass

    @abstractmethod
    def post(self, url, timeout=10, headers=None, cookies=None, data=None):
        pass
