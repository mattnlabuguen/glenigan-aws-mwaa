import base64
import logging
from datetime import datetime


from bs4 import BeautifulSoup

from strategies.base.crawl_strategy import CrawlStrategy
from strategies.downloader.default import DefaultDownloader


class HillingdonGovUkCrawlStrategy(CrawlStrategy):
    def __init__(self):
        self.download = DefaultDownloader().download
        self.base_url = 'https://planning.hillingdon.gov.uk/OcellaWeb'
        self.search_url = f'{self.base_url}/planningSearch'

    def crawl(self, source: str) -> dict:
        """
        Crawl data from source
        :param source: str - source url
        :return: dict - crawled data
        """
        try:
            logging.info(f'Getting data from {source}')
            main_page_data = self.download(source)
            if main_page_data:
                main_page_soup = BeautifulSoup(main_page_data, 'lxml')
                planning_application_data = {
                    'application_details': {'data': main_page_data, 'source': source},
                    'application_form_document': self._get_planning_application_document(main_page_soup),
                    'date_captured': datetime.now().strftime('%Y-%m-%dT%H%M%S')
                }
                return planning_application_data

        except Exception as e:
            error_message = f'crawl() error: {str(e)}'
            logging.error(error_message)

    def _get_planning_application_document(self, soup: BeautifulSoup) -> dict:
        """
        Get planning application document
        :param soup: BeautifulSoup
        :return: dict - planning application document data
        """
        application_document_data = {
            'data': None,
            'source': None
        }
        document_button_tag = soup.select_one('form[name="showDocuments"][action]')

        if not document_button_tag:
            logging.info('No document button found')
            return application_document_data

        document_page_url = f"{self.base_url}/{document_button_tag['action']}"
        try:
            document_page_data = self.download(document_page_url)

            if not document_page_data:
                return application_document_data

            document_page_soup = BeautifulSoup(document_page_data, 'lxml')
            document_link_tag = document_page_soup.select_one('a[target="showdocument"][href]')

            if not document_link_tag:
                return application_document_data

            document_url = f"{self.base_url}/{document_link_tag['href']}"
            document_data = self.download(document_url, is_document=True)

            if document_data:
                encoded_data = base64.b64encode(document_data).decode('utf-8')
                application_document_data['data'] = encoded_data
                application_document_data['source'] = document_url

        except Exception as e:
            error_message = f'_get_planning_application_document() error: {str(e)}'
            logging.error(error_message)

        return application_document_data
