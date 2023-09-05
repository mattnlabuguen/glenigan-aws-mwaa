import logging
from datetime import datetime, timedelta
from urllib.parse import urlencode, quote_plus

from bs4 import BeautifulSoup

from strategies.base.source_strategy import SourceStrategy
from strategies.downloader.default import DefaultDownloader


class HillingdonGovUkSourceStrategy(SourceStrategy):
    def __init__(self):
        self.download = DefaultDownloader().download
        self.base_url = 'https://planning.hillingdon.gov.uk/OcellaWeb'
        self.search_url = f'{self.base_url}/planningSearch'
        self.post_request_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
                      "*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": "planning.hillingdon.gov.uk",
            "Origin": "https://planning.hillingdon.gov.uk",
            "Referer": "https://planning.hillingdon.gov.uk/OcellaWeb/planningSearch",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/116.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"'
        }

    def get_sources(self, months_ago=2) -> list:
        """
        Get sources from hillingdon.gov.uk
        :param months_ago: int - number of months ago
        :return: list - list of sources
        """
        sources = []

        start_date = datetime.now() - timedelta(days=months_ago * 30)
        end_date = datetime.now()

        if months_ago > 2:
            current_date = start_date
            while current_date < end_date:
                batch_end_date = current_date + timedelta(days=60)
                if batch_end_date > end_date:
                    batch_end_date = end_date
                sources.extend(self._batch_request(current_date, batch_end_date))
                current_date = batch_end_date + timedelta(days=1)
        else:
            from_date = datetime.now() - timedelta(days=months_ago * 30)
            to_date = datetime.now()
            sources.extend(self._batch_request(from_date, to_date))

        logging.info(f'Removing duplicates...')
        sources = list(set(sources))
        logging.info(f'Found {len(sources)} sources.')

        return sources

    def _batch_request(self, start_date, end_date) -> list:
        """
        Batch requests to get sources
        :param start_date: datetime
        :param end_date: datetime
        :return:
        """
        sources = []
        initial_form_data = ('action=Search&showall=showall&reference=&location=&OcellaPlanningSearch.postcode=&'
                             'area=&applicant=&agent=&undecided=&type=&')

        form_data = {
            'receivedFrom': start_date.strftime('%d-%m-%y'),
            'receivedTo': end_date.strftime('%d-%m-%y'),
            'decidedFrom': start_date.strftime('%d-%m-%y'),
            'decidedTo': end_date.strftime('%d-%m-%y')
        }

        complete_form_data = initial_form_data + urlencode(form_data, quote_via=quote_plus)
        try:
            logging.info('Getting sources...')
            search_data = self.download(url=self.search_url, headers=self.post_request_headers, data=complete_form_data)
            if search_data:
                search_data_soup = BeautifulSoup(search_data, 'lxml')
                sources = self._get_search_data(search_data_soup)
                logging.info(f'Found {len(sources)} sources from {start_date.strftime("%d-%m-%y")} '
                             f'to {end_date.strftime("%d-%m-%y")}')

        except Exception as e:
            error_message = f'get_sources() error: {str(e)}'
            logging.error(error_message)
            raise Exception(error_message)

        return sources

    def _get_search_data(self, soup: BeautifulSoup) -> list:
        """
        Get search data from soup
        :param soup: BeautifulSoup object
        :return: list
        """
        try:
            search_data = soup.select('tr[style="vertical-align:top"] > td[style="white-space:nowrap"] > a[href]')
            if search_data:
                search_data = [f"{self.base_url}/{tag['href']}" for tag in search_data]
        except Exception as e:
            error_message = f'_get_search_data() error: {str(e)}'
            logging.error(error_message)
            raise Exception(error_message)

        return search_data
