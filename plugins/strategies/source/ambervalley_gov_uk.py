import json
import logging
from datetime import datetime, timedelta

from strategies.base.source_strategy import SourceStrategy
from strategies.downloader.zyte import ZyteDownloader


class AmbervalleyGovUkSourceStrategy(SourceStrategy):
    def __init__(self):
        self.download = ZyteDownloader(country='uk').download
        self.base_url = 'https://info.ambervalley.gov.uk/WebServices/AVBCFeeds'

        self.default_headers = {
            "Host": "info.ambervalley.gov.uk",  # This is a required header
            "Origin": "https://www.ambervalley.gov.uk",
            "Referer": "https://www.ambervalley.gov.uk/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }
        self.post_request_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Host": "info.ambervalley.gov.uk",  # This is a required header
            "Origin": "https://www.ambervalley.gov.uk",
            "Referer": "https://www.ambervalley.gov.uk/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

    def get_sources(self, months_ago: int = 6) -> list:
        """
        Get the reference numbers for the planning applications.
        :param months_ago: The number of months to go back to get the reference numbers.
        :return: A list of reference numbers.
        """
        logging.info('Getting reference numbers...')
        sources = []
        try:
            date_start = datetime.now() - timedelta(days=30 * months_ago)
            date_end = datetime.now()
            # If the months_ago value is greater than 4, we need to split the request into multiple requests
            # because the server times out if the request is too long.
            if months_ago > 4:
                for i in range(0, months_ago, 4):
                    date_start = date_start + timedelta(days=30 * i)
                    date_end = date_start + timedelta(days=30 * (i + 4))
                    sources.extend(self._get_reference_numbers(date_start, date_end))
            else:
                sources.extend(self._get_reference_numbers(date_start, date_end))

        except Exception as e:
            error_message = f'get_sources() error: {str(e)}'
            logging.error(error_message)
            raise Exception(error_message)

        return sources

    def _get_reference_numbers(self, date_start: datetime, date_end: datetime) -> list:
        """
        Get the reference numbers for the planning applications.
        :param date_start: datetime object of the start date.
        :param date_end: datetime object of the end date.
        :return: a list of reference numbers.
        """
        reference_numbers = []
        try:
            request_path = '/DevConJSON.asmx/PlanAppsByAddressKeyword'
            request_url = f'{self.base_url}{request_path}'

            from_date = date_start.strftime('%d/%b/%Y')
            to_date = date_end.strftime('%d/%b/%Y')

            form_data = f"keyWord=&fromDate={from_date}&toDate={to_date}"
            logging.info(f'Requesting data from {from_date} to {to_date} to {request_url}')
            search_data = self.download(request_url, headers=self.post_request_headers, timeout=300000, data=form_data)

            if search_data:
                json_data = json.loads(search_data)
                if json_data and isinstance(json_data, list):
                    reference_numbers = [data['refVal'] for data in json_data if 'refVal' in data and data['refVal']]
                    logging.info(f'Found {len(reference_numbers)} reference numbers')
            else:
                raise Exception('No data found')
        except Exception as e:
            error_message = f'_get_reference_numbers() error: {str(e)}'
            logging.error(error_message)
            raise Exception(error_message)

        return reference_numbers
