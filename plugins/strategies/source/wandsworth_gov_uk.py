import logging
import random
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode, quote_plus

from bs4 import BeautifulSoup

from strategies.base.source_strategy import SourceStrategy
from strategies.downloader.zyte import ZyteDownloader
from utils.bs_utils import get_tag_attribute, get_aspnet_variables


class WandsworthGovUkSourceStrategy(SourceStrategy):
    def __init__(self):
        self.download = ZyteDownloader(country='uk').download
        self.base_application_url = 'https://planning.wandsworth.gov.uk/Northgate/PlanningExplorer/Generic/'
        self.general_search_url = 'https://planning.wandsworth.gov.uk/Northgate/PlanningExplorer/GeneralSearch.aspx'

        self.default_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
                      '*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://planning.wandsworth.gov.uk',
            'Referer': 'https://planning.wandsworth.gov.uk/Northgate/PlanningExplorer/GeneralSearch.aspx',
            'Cache-Control': 'max-age=0',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
        }
        self.post_request_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
                      '*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://planning.wandsworth.gov.uk',
            'Referer': 'https://planning.wandsworth.gov.uk/Northgate/PlanningExplorer/GeneralSearch.aspx',
            'Cache-Control': 'max-age=0',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
        }

    def get_sources(self, months_ago: int = 6) -> list:
        """
        Get all planning application sources from planning.wandsworth.gov.uk
        :param months_ago: range of months to search
        :return: list of planning application sources
        """
        date_start = datetime.now() - timedelta(days=30 * months_ago)
        date_end = datetime.now()

        viewstate = None
        viewstate_generator = None
        event_validation = None

        try:
            logging.info('Getting general search data...')
            general_search_url_data = self.download(self.general_search_url, headers=self.default_headers)
            if general_search_url_data:
                general_search_url_soup = BeautifulSoup(general_search_url_data, 'lxml')
                viewstate, viewstate_generator, event_validation = get_aspnet_variables(general_search_url_soup)

            if all([viewstate, viewstate_generator, event_validation]):
                logging.info('Getting first page data...')
                first_page_form_data = self._format_first_page_form_data(viewstate,
                                                                         viewstate_generator,
                                                                         event_validation)
                first_page_data = self._get_first_page_data(first_page_form_data, date_start, date_end)
            else:
                raise Exception('Missing ASP.NET variables from general search URL '
                                '(viewstate, viewstate_generator, or event_validation)')

            if first_page_data:
                planning_application_sources = self._get_planning_application_sources(first_page_data)
            else:
                raise Exception('Failed to get first page data')

        except Exception as e:
            error_message = f'get_sources() error: {str(e)}'
            logging.error(error_message)
            raise Exception(error_message)

        return planning_application_sources

    def _get_first_page_data(self, first_page_form_data: str, date_start: datetime, date_end: datetime, ) -> str:
        """
        Get the first page of the search results
        :param first_page_form_data: formatted first page form data
        :param date_start: start date of the search
        :param date_end: end date of the search
        :return: HTML data from the first page of the search results
        """
        from_date = date_start.strftime('%d/%m/%Y')
        to_date = date_end.strftime('%d/%m/%Y')

        search_form_data = {
            'cboSelectDateValue': 'DATE_RECEIVED',
            'cboMonths': '1',
            'cboDays': '1',
            'rbGroup': 'rbRange',
            'dateStart': from_date,
            'dateEnd': to_date,
            'csbtnSearch': 'Search',
        }
        logging.info(f'Requesting data from {from_date} to {to_date} to {search_form_data}')
        complete_form_data = f'{first_page_form_data}&{urlencode(search_form_data)}'
        first_page_data = self.download(self.general_search_url, timeout=100, headers=self.post_request_headers,
                                        data=complete_form_data)

        return first_page_data

    def _get_planning_application_sources(self, first_page_data: str) -> list:
        """
        Get all planning application sources from the first page of the search results
        :param first_page_data: HTML data from the first page of the search results
        :return: list of planning application sources
        """
        logging.info(f'Getting all planning application sources...')

        first_page_soup = BeautifulSoup(first_page_data, 'lxml')
        planning_application_sources = self._get_search_result_data(first_page_soup)
        next_url = self._get_next_url(first_page_soup)
        current_page = 1

        while next_url:
            logging.info(f'On page {current_page}: {next_url}')
            page_data = self.download(next_url, timeout=random.randint(100, 1000), headers=self.default_headers)
            if page_data:
                page_soup = BeautifulSoup(page_data, 'lxml')
                planning_application_sources.extend(self._get_search_result_data(page_soup))

                next_url = self._get_next_url(page_soup)
                if not next_url:
                    logging.info(f'Next page not found')
                    break
                else:
                    current_page += 1
            else:
                logging.info(f'No data found for {next_url}')
                break

        logging.info(f'Found {len(planning_application_sources)} applications')

        return planning_application_sources

    def _get_search_result_data(self, soup: BeautifulSoup) -> list:
        """
        Get all planning application sources from the search results
        :param soup: BeautifulSoup object
        :return: list of planning application URLs
        """
        search_results = []
        page_links = soup.select('td.TableData a.data_text')

        for link in page_links:
            href = get_tag_attribute(link, 'href')
            if href:
                href = re.sub(r'\s', '', href.replace(' ', '%20'))
                search_results.append(f'{self.base_application_url}{href}')

        return search_results

    def _get_next_url(self, soup: BeautifulSoup) -> str:
        """
        Get the next page URL from the search results
        :param soup: BeautifulSoup object
        :return: next page URL
        """
        next_url = None
        next_url_tag = None
        href = None

        child_tag = soup.select_one('a.noborder img[title="Go to next page "]')
        if child_tag:
            next_url_tag = child_tag.parent

        if next_url_tag:
            href = get_tag_attribute(next_url_tag, 'href')

        if href:
            href = re.sub(r'\s', '', href.replace(' ', '%20'))
            next_url = f'{self.base_application_url}{href}'

        return next_url

    @staticmethod
    def _get_aspnet_variables(soup: BeautifulSoup) -> tuple:
        """
        Get ASP.NET variables from a BeautifulSoup object if they exist
        :param soup: BeautifulSoup object
        :return: tuple of ASP.NET variables
        """
        viewstate = None
        viewstate_generator = None
        event_validation = None

        if soup:
            viewstate_tag = soup.select_one('input#__VIEWSTATE')
            viewstate_generator_tag = soup.select_one('input#__VIEWSTATEGENERATOR')
            event_validation_tag = soup.select_one('input#__EVENTVALIDATION')

            if viewstate_tag:
                viewstate = get_tag_attribute(viewstate_tag, 'value')

            if viewstate_generator_tag:
                viewstate_generator = get_tag_attribute(viewstate_generator_tag, 'value')

            if event_validation_tag:
                event_validation = get_tag_attribute(event_validation_tag, 'value')

        return viewstate, viewstate_generator, event_validation

    @staticmethod
    def _format_first_page_form_data(viewstate: str, viewstate_generator: str, event_validation: str) -> str:
        """
        Format the first page form data
        :param viewstate: __VIEWSTATE
        :param viewstate_generator: __VIEWSTATEGENERATOR
        :param event_validation: __EVENTVALIDATION
        :return: formatted first page form data
        """
        first_page_form_data = f'__VIEWSTATE={quote_plus(viewstate)}' \
                               f'&__VIEWSTATEGENERATOR={quote_plus(viewstate_generator)}' \
                               f'&__EVENTVALIDATION={quote_plus(event_validation)}' \
                               f'&txtApplicationNumber=&txtApplicantName=&txtAgentName=&cboStreetReferenceNumber=' \
                               f'&txtProposal=&edrDateSelection=&cboWardCode=&cboParishCode=&cboApplicationTypeCode=' \
                               f'&cboDevelopmentTypeCode=&cboStatusCode=&'

        return first_page_form_data
