import json
import logging
import os
import requests
import urllib3
from requests.exceptions import RequestException, Timeout, TooManyRedirects, HTTPError

from retrying import retry

from strategies.base.download_strategy import DownloadStrategy

urllib3.disable_warnings()


class ZyteDownloader(DownloadStrategy):
    def __init__(self, country=None, port='8011'):
        self.requester = requests.Session()
        self.requester.verify = self.get_cert_path()
        country_key = self.get_country_key(country)
        if country_key:
            logging.info(f"Using proxy for {country}")
            self.requester.proxies = {
                "http": f"http://{country_key}:@proxy.zyte.com:{port}/",
                "https": f"http://{country_key}:@proxy.zyte.com:{port}/",
            }

    @retry(
        stop_max_attempt_number=5,
        retry_on_exception=lambda e: isinstance(e, (Timeout, HTTPError)),
        wait_random_min=5000,
        wait_random_max=10000
    )
    def download(self, url, timeout=100, headers=None, cookies=None, data=None, is_document=False):
        raw_data = None

        try:
            if not data:
                response = self.requester.get(url, timeout=timeout, headers=headers, cookies=cookies)
            else:
                response = self.requester.post(url, timeout=timeout, headers=headers, cookies=cookies, data=data)

            response.raise_for_status()

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

    @staticmethod
    def get_country_key(country=None):
        logging.info('Getting country key')
        try:
            downloader_dir = os.path.dirname(os.path.abspath(__file__))
            key_file_path = os.path.join(downloader_dir, 'proxy_keys.json')

            with open(key_file_path, "r") as file:
                keys = json.load(file)

            # Default country is UK
            if country is None:
                country = 'uk'
            else:
                country = country.lower()

            country_key = keys[country]

        except KeyError:
            raise KeyError(f'Country {country} is not supported')

        return country_key

    @staticmethod
    def get_cert_path():
        logging.info('Getting cert path')
        try:
            downloader_dir = os.path.dirname(os.path.abspath(__file__))
            cert_file_path = os.path.join(downloader_dir, 'zyte-ca.crt')
        except Exception as e:
            raise e

        return cert_file_path
