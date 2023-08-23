import json
import logging
import os
import requests
from requests.exceptions import HTTPError, ConnectionError

import urllib3
from retrying import retry

from scripts.base.downloader import DownloaderStrategy

urllib3.disable_warnings()


class ZyteDownloader(DownloaderStrategy):
    max_retries = 5
    retry_delay = 5000 # In milliseconds

    def __init__(self, country=None, port='8011'):
        self.requester = requests.Session()
        self.requester.verify = self.get_cert_path()
        country_key = self.get_country_key(country)
        if country_key:
            logging.info(f"Using proxy for {country_key}")
            self.requester.proxies = {
                "http": f"http://{country_key}:@proxy.zyte.com:{port}/",
                "https": f"http://{country_key}:@proxy.zyte.com:{port}/",
            }

    @retry(stop_max_attempt_number=max_retries, wait_fixed=retry_delay, retry_on_exception=ConnectionError)
    def get(self, url, timeout=100, headers=None, cookies=None):
        try:
            response = self.requester.get(url, timeout=timeout, headers=headers, cookies=cookies)
            response.raise_for_status()

            return response
        except ConnectionError:
            raise ConnectionError
        except HTTPError:
            raise HTTPError

    @retry(stop_max_attempt_number=max_retries, wait_fixed=retry_delay)
    def post(self, url, timeout=100, headers=None, cookies=None, data=None):
        try:
            response = self.requester.post(url, timeout=timeout, headers=headers, cookies=cookies, data=data)
            response.raise_for_status()

            return response
        except ConnectionError:
            raise ConnectionError
        except HTTPError:
            raise HTTPError

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