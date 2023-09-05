import logging
import requests
import urllib3
from requests.exceptions import RequestException, Timeout, TooManyRedirects, HTTPError

from retrying import retry

from strategies.base.download_strategy import DownloadStrategy


urllib3.disable_warnings()


class DefaultDownloader(DownloadStrategy):
    def __init__(self):
        self.requester = requests.Session()
        self.requester.verify = False

    @retry(
        stop_max_attempt_number=5,
        retry_on_exception=lambda e: isinstance(e, (Timeout, HTTPError)),
        wait_random_min=5000,
        wait_random_max=10000
    )
    def download(self, url, headers=None, cookies=None, data=None, timeout=100, is_document=False):
        raw_data = None

        try:
            if not data:
                response = self.requester.get(url, timeout=timeout, headers=headers, cookies=cookies)
            else:
                response = self.requester.post(url, timeout=timeout, headers=headers, cookies=cookies, data=data)

            if is_document and response:
                if 'application/pdf' in response.headers.get('Content-Type', ''):
                    raw_data = response.content

            elif not is_document and response:
                raw_data = response.text

        except Timeout:
            logging.error(f"Timeout occurred while downloading {url}")
            return None
        except TooManyRedirects:
            logging.error(f"Too many redirects for {url}")
            return None
        except HTTPError as e:
            logging.error(f"HTTP Error {e.response.status_code} occurred for {url}")
            return None
        except RequestException as e:
            logging.error(f"An error occurred while downloading {url}: {e}")
            return None

        return raw_data
