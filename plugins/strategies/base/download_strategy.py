from abc import ABC, abstractmethod


class DownloadStrategy(ABC):
    @abstractmethod
    def download(self, **kwargs):
        pass
