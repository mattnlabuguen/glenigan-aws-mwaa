import logging
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode, quote_plus

from bs4 import BeautifulSoup

from scripts.strategies.base.crawler import CrawlingStrategy
from scripts.strategies.downloader.default_downloader import DefaultDownloader
from scripts.strategies.utils.bs4_utils import clean_href, get_href


class WandsworthGovUkCrawlingStrategy(CrawlingStrategy):
    def __init__(self):
        self.downloader = DefaultDownloader()
        self.base_application_url = 'https://planning.wandsworth.gov.uk/Northgate/PlanningExplorer/Generic/'
        self.general_search_url = 'https://planning.wandsworth.gov.uk/Northgate/PlanningExplorer/GeneralSearch.aspx'

        self.post_request_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
                      '*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://planning.wandsworth.gov.uk',
            'Referer': 'https://planning.wandsworth.gov.uk/Northgate/PlanningExplorer/GeneralSearch.aspx',
            'Cache-Control': 'max-age=0',
            'Content-Type': 'application/x-www-form-urlencoded',
        }

    def download(self, url, timeout=100, headers=None, cookies=None, data=None, is_document=False):
        """
        :param url: The URL to download content from.
        :param timeout: The timeout for the request in seconds.
        :param headers: Custom headers to be included in the request.
        :param cookies: Cookies to be included in the request.
        :param data: Data to be sent in the request body *(for POST requests)*.
        :param is_document: Boolean to check what type of content the method returns.
        :return: Returns downloaded content from the URL *(in bytes or string)*.
        """
        raw_data = None

        if not isinstance(headers, dict):
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
                          '*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.wandsworth.gov.uk/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/115.0.0.0 Safari/537.36',
            }

        try:
            if not data:
                response = self.downloader.get(url, timeout=timeout, headers=headers, cookies=cookies)
            else:
                response = self.downloader.post(url, timeout=timeout, headers=headers, cookies=cookies, data=data)

            if is_document and response:
                if 'application/pdf' in response.headers.get('Content-Type', ''):
                    raw_data = response.content

            elif not is_document and response:
                raw_data = response.text

        except Exception as e:
            logging.error(f'download() error: {str(e)}')

        return raw_data

    def get_sources(
            self,
            date_start: datetime = datetime.now() - timedelta(days=6 * 30),  # Six months ago
            date_end: datetime = datetime.now(),
            max_pages=10
    ) -> list:
        planning_application_sources = []
        viewstate = None
        viewstate_generator = None
        event_validation = None

        try:
            logging.info('Getting general search data...')
            general_search_url_data = self.download(self.general_search_url)
            if general_search_url_data:
                general_search_url_soup = BeautifulSoup(general_search_url_data, 'lxml')
                viewstate, viewstate_generator, event_validation = self._get_aspnet_variables(general_search_url_soup)

            if all([viewstate, viewstate_generator, event_validation]): # Check if values are not None
                logging.info('Getting first page data...')
                first_page_form_data = self._get_first_page_form_data(viewstate, viewstate_generator, event_validation)
                first_page_data = self._get_first_page_data(first_page_form_data, date_start, date_end)
            else:
                raise Exception('Missing ASP.NET variables from general search URL '
                                '(viewstate, viewstate_generator, or event_validation)')

            if first_page_data:
                planning_application_sources = self._get_planning_application_sources(first_page_data,
                                                                                      max_pages=max_pages)
            else:
                raise Exception('Failed to get first page data')

        except Exception as e:
            logging.error(f'get_sources() error: {str(e)}')

        return planning_application_sources

    def _get_first_page_data(
            self,
            first_page_form_data: str,
            date_start: datetime,
            date_end: datetime,
    ) -> str:

        search_form_data = {
            'cboSelectDateValue': 'DATE_RECEIVED',
            'cboMonths': '1',
            'cboDays': '1',
            'rbGroup': 'rbRange',
            'dateStart': date_start.strftime('%d/%m/%Y'),
            'dateEnd': date_end.strftime('%d/%m/%Y'),
            'csbtnSearch': 'Search',
        }

        complete_form_data = f'{first_page_form_data}&{urlencode(search_form_data)}'
        first_page_data = self.download(self.general_search_url, headers=self.post_request_headers,
                                        data=complete_form_data)

        return first_page_data

    def crawl(self, planning_application_sources: list) -> list:
        logging.info('Getting all page data from each source...')
        raw_data_list = []
        try:
            for source in planning_application_sources:
                logging.info(f'Page: {source}')
                planning_application_data = {
                    'main_page_data': None,
                    'dates_page_data': None,
                    'application_form_document_data': None,
                    'source': source,
                    'date_captured': datetime.now().strftime('%Y-%m-%dT%H%M%S')
                }

                main_page_data = self.download(source)

                if main_page_data:
                    planning_application_data['main_page_data'] = main_page_data
                    main_page_soup = BeautifulSoup(main_page_data, 'lxml')

                    planning_application_data['dates_page_data'] = self._get_dates_page_data(main_page_soup)
                    document_urls, planning_application_data['application_form_document_data'] = \
                        self._get_document_data(main_page_soup)

                    if document_urls:
                        planning_application_data.update(document_urls)

            raw_data_list.append(planning_application_data)
        except Exception as e:
            logging.error(f'crawl() error: {str(e)}')

        return raw_data_list

    def _get_planning_application_sources(self, first_page_data: str, max_pages: int = 10) -> list:
        logging.info(f'Getting all planning application sources until page {max_pages}')

        first_page_soup = BeautifulSoup(first_page_data, 'lxml')
        planning_application_sources = self._get_search_result_data(first_page_soup)
        next_url = self._get_next_url(first_page_soup)
        current_page = 1

        while current_page < max_pages:
            logging.info(f'On page {current_page}')
            page_data = self.download(next_url)
            if page_data:
                page_soup = BeautifulSoup(page_data, 'lxml')
                planning_application_sources.extend(self._get_search_result_data(page_soup))

                next_url = self._get_next_url(page_soup)
                if not next_url:
                    logging.info(f'Next page not found')
                    break
                else:
                    current_page += 1

        logging.info(f'Found {len(planning_application_sources)} applications')

        return planning_application_sources

    def _get_search_result_data(self, soup) -> list:
        search_results = []
        page_links = soup.select('td.TableData a.data_text')

        for link in page_links:
            search_results.append(f'{self.base_application_url}{clean_href(link["href"])}')

        return search_results

    def _get_next_url(self, soup: BeautifulSoup) -> str:
        next_url = None
        next_url_tag = None
        child_tag = soup.select_one('a.noborder img[title="Go to next page "]')
        if child_tag:
            next_url_tag = child_tag.parent

        if next_url_tag and next_url_tag.has_attr('href'):
            next_url = f'{self.base_application_url}{clean_href(next_url_tag["href"])}'

        return next_url

    def _get_dates_page_data(self, soup: BeautifulSoup):
        dates_page_href = get_href(soup, 'a[title="Link to the application Dates page."]')
        dates_page_url = f'{self.base_application_url}{clean_href(dates_page_href)}'

        if dates_page_url:
            dates_page_data = self.download(dates_page_url)

        return dates_page_data

    def _get_document_data(self, soup: BeautifulSoup):
        document_urls = {}
        documents_page_url = get_href(soup, 'a[title="Link to View Related Documents"]')
        if documents_page_url:
            documents_page_data = self.download(documents_page_url)
            if documents_page_data:
                document_urls = self._get_document_urls(documents_page_data)

        if document_urls and 'application_form_urls' in document_urls:
            application_form_urls = document_urls['application_form_urls']
            for application_form in application_form_urls:
                application_form_document_data = self.download(application_form, is_document=True)
                if application_form_document_data and isinstance(application_form_document_data, bytes):
                    # Take the first instance of returned data that is in bytes.
                    break
        else:
            logging.info('No documents for this planning application')

        return document_urls, application_form_document_data

    def _get_document_urls(self, page_data: str) -> dict:
        document_urls = {}
        document_event_targets = {}
        case_no = None

        soup = BeautifulSoup(page_data, 'lxml')

        viewstate, viewstate_generator, event_validation = self._get_aspnet_variables(soup)
        case_no_tag = soup.select_one('span#lblCaseNo')
        document_type_tags = soup.select('span[id*="lblChoice"]')

        if document_type_tags:
            document_event_targets = {}
            for tag in document_type_tags:
                document_type_title = f"{tag.get_text().strip().replace(' ', '_').lower()}_urls"
                event_target_pattern = r'gvDocs\$ctl\d+\$lnkDShow'
                event_target_tag = tag.find_parent('tr').select_one('a')

                if event_target_tag and event_target_tag.has_attr('href'):
                    event_target_match = re.search(event_target_pattern, event_target_tag['href'])
                    if event_target_match:
                        event_target = event_target_match.group(0)
                        document_event_targets.update({document_type_title: event_target})

        if case_no_tag:
            case_no = case_no_tag.get_text()

        if all([document_event_targets, case_no, viewstate, viewstate_generator, event_validation]):
            for document_title, event_target in document_event_targets.items():
                form_data = f'__EVENTTARGET={quote_plus(event_target)}' \
                            f'&__EVENTARGUMENT=' \
                            f'&__VIEWSTATE={quote_plus(viewstate)}' \
                            f'&__VIEWSTATEGENERATOR={quote_plus(viewstate_generator)}' \
                            f'&__SCROLLPOSITIONX=0&__SCROLLPOSITIONY=0' \
                            f'&__EVENTVALIDATION={quote_plus(event_validation)}'

                page_url = f'https://planning2.wandsworth.gov.uk/planningcase/comments.aspx?case={quote_plus(case_no)}'

                headers = self.post_request_headers
                headers['Origin'] = 'https://planning2.wandsworth.gov.uk'
                headers['Referer'] = page_url

                post_page_data = self.download(page_url, headers=self.post_request_headers, data=form_data)
                if post_page_data:
                    post_page_soup = BeautifulSoup(post_page_data, 'lxml')
                    document_tags = post_page_soup.select('a[target="_blank"]')
                    document_urls.update({document_title: [tag['href'] for tag in document_tags
                                                           if tag and tag.has_attr('href')]})

        return document_urls

    @staticmethod
    def _get_aspnet_variables(soup: BeautifulSoup) -> tuple:
        viewstate = None
        viewstate_generator = None
        event_validation = None

        if soup:
            viewstate_tag = soup.select_one('input#__VIEWSTATE')
            viewstate_generator_tag = soup.select_one('input#__VIEWSTATEGENERATOR')
            event_validation_tag = soup.select_one('input#__EVENTVALIDATION')

            if viewstate_tag and viewstate_tag.has_attr('value'):
                viewstate = viewstate_tag['value']

            if viewstate_generator_tag and viewstate_generator_tag.has_attr('value'):
                viewstate_generator = viewstate_generator_tag['value']

            if event_validation_tag and event_validation_tag.has_attr('value'):
                event_validation = event_validation_tag['value']

        return viewstate, viewstate_generator, event_validation

    @staticmethod
    def _get_first_page_form_data(
            viewstate: str,
            viewstate_generator: str,
            event_validation: str
    ) -> str:
        first_page_form_data = f'__VIEWSTATE={quote_plus(viewstate)}' \
                               f'&__VIEWSTATEGENERATOR={quote_plus(viewstate_generator)}' \
                               f'&__EVENTVALIDATION={quote_plus(event_validation)}' \
                               f'&txtApplicationNumber=&txtApplicantName=&txtAgentName=&cboStreetReferenceNumber=' \
                               f'&txtProposal=&edrDateSelection=&cboWardCode=&cboParishCode=&cboApplicationTypeCode=' \
                               f'&cboDevelopmentTypeCode=&cboStatusCode=&'

        return first_page_form_data
