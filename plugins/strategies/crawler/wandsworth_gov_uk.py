import logging
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode, quote_plus

from bs4 import BeautifulSoup

from plugins.strategies.crawler.crawler import CrawlingStrategy
from plugins.strategies.downloader.default_downloader import DefaultDownloader
from plugins.strategies.utils.bs4_utils import clean_href, get_application_href


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

    def download(self, url, timeout=100, headers=None, cookies=None, data=None):
        """
        :param url: The URL to download content from.
        :param timeout: The timeout for the request in seconds.
        :param headers: Custom headers to be included in the request.
        :param cookies: Cookies to be included in the request.
        :param data: Data to be sent in the request body *(for POST requests)*.
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

            if response:
                raw_data = response.text

        except Exception as e:
            logging.error(f'download() error: {str(e)}')

        return raw_data

    def download_document(self, url, timeout=100, headers=None, cookies=None, data=None):
        """
        :param url: The URL to download content from.
        :param timeout: The timeout for the request in seconds.
        :param headers: Custom headers to be included in the request.
        :param cookies: Cookies to be included in the request.
        :param data: Data to be sent in the request body *(for POST requests)*.
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

            if response:
                if 'application/pdf' in response.headers.get('Content-Type', ''):
                    raw_data = response.content

        except Exception as e:
            logging.error(f'download_document() error: {str(e)}')

        return raw_data

    def crawl(self, urls=None, max_pages=10) -> list:
        """
        Crawls through the Wandsworth Planning Application directory and extracts information from all pages needing to
        be extracted.
        :return: Returns a list of dictionaries containing raw data from pages crawled.
        """
        if not urls:
            logging.info('Getting general search data...')
            viewstate, viewstate_generator, event_validation = self._get_general_search_data()

            logging.info('Getting first page data...')
            first_page_data = self._get_first_page_data(viewstate, viewstate_generator, event_validation)

            logging.info(f'Getting all application URLs until page {max_pages}')
            application_urls = self._get_application_urls(first_page_data, max_pages=max_pages)
            application_urls = [f'{self.base_application_url}{url}' for url in application_urls]
            logging.info(f'Found {len(application_urls)} applications')
        else:
            logging.info('Testing URLs...')
            application_urls = urls

        logging.info('Getting all page data from each application...')
        all_page_raw_data_list = self._get_all_page_raw_data(application_urls)

        return all_page_raw_data_list

    def _get_general_search_data(self) -> tuple:
        base_url_data = self.download(self.general_search_url)

        viewstate = None
        viewstate_generator = None
        event_validation = None

        if base_url_data:
            base_url_soup = BeautifulSoup(base_url_data, 'lxml')
            viewstate_tag = base_url_soup.select_one('input#__VIEWSTATE')
            viewstate_generator_tag = base_url_soup.select_one('input#__VIEWSTATEGENERATOR')
            event_validation_tag = base_url_soup.select_one('input#__EVENTVALIDATION')

            if viewstate_tag and viewstate_tag.has_attr('value'):
                viewstate = viewstate_tag['value']

            if viewstate_generator_tag and viewstate_generator_tag.has_attr('value'):
                viewstate_generator = viewstate_generator_tag['value']

            if event_validation_tag and event_validation_tag.has_attr('value'):
                event_validation = event_validation_tag['value']

        return viewstate, viewstate_generator, event_validation

    def _get_first_page_data(self, viewstate: str, viewstate_generator: str, event_validation: str) -> str:
        current_date_time = datetime.now()
        six_months_ago = current_date_time - timedelta(days=6 * 30)

        default_form_data = f'__VIEWSTATE={quote_plus(viewstate)}' \
                            f'&__VIEWSTATEGENERATOR={quote_plus(viewstate_generator)}' \
                            f'&__EVENTVALIDATION={quote_plus(event_validation)}' \
                            f'&txtApplicationNumber=&txtApplicantName=&txtAgentName=&cboStreetReferenceNumber=' \
                            f'&txtProposal=&edrDateSelection=&cboWardCode=&cboParishCode=&cboApplicationTypeCode=' \
                            f'&cboDevelopmentTypeCode=&cboStatusCode=&'
        form_data = {
            'cboSelectDateValue': 'DATE_RECEIVED',
            'cboMonths': '1',
            'cboDays': '1',
            'rbGroup': 'rbRange',
            'dateStart': six_months_ago.strftime('%d/%m/%Y'),
            'dateEnd': current_date_time.strftime('%d/%m/%Y'),
            'csbtnSearch': 'Search',
        }
        complete_form_data = f'{default_form_data}&{urlencode(form_data)}'
        first_page_data = self.download(self.general_search_url, headers=self.post_request_headers,
                                        data=complete_form_data)

        return first_page_data

    def _get_application_urls(self, first_page_data: str, max_pages: int = 10) -> list:
        first_page_soup = BeautifulSoup(first_page_data, 'lxml')
        application_urls = self._get_search_result_data(first_page_soup)
        next_url = self._get_next_url(first_page_soup)
        current_page = 1

        while current_page < max_pages:
            logging.info(f'On page {current_page}')
            page_data = self.download(next_url)
            if page_data:
                page_soup = BeautifulSoup(page_data, 'lxml')
                application_urls.extend(self._get_search_result_data(page_soup))

                next_url = self._get_next_url(page_soup)
                if not next_url:
                    logging.info(f'Next page not found')
                    break
                else:
                    current_page += 1

        return application_urls

    def _get_next_url(self, soup: BeautifulSoup) -> str:
        next_url = None
        next_url_tag = None
        # Change to 'Go to last page' for testing applications with PDFs.
        child_tag = soup.select_one('a.noborder img[title="Go to next page "]')
        if child_tag:
            next_url_tag = child_tag.parent

        if next_url_tag and next_url_tag.has_attr('href'):
            next_url = f'{self.base_application_url}{clean_href(next_url_tag["href"])}'

        return next_url

    def _get_all_page_raw_data(self, application_urls: list):
        all_page_raw_data = []
        for url in application_urls:
            logging.info(f'Page: {url}')
            application_data = {
                'main_details_data': None,
                'dates_data': None,
                'document_data': None,
                'source': url
            }
            document_urls = None
            document_data = None
            application_main_data = self.download(url)
            if application_main_data:
                application_data['main_details_data'] = application_main_data
                application_soup = BeautifulSoup(application_main_data, 'lxml')

                application_date_href = get_application_href(application_soup, 'a[title="Link to the '
                                                                               'application Dates page."]')

                application_dates_url = f'{self.base_application_url}{clean_href(application_date_href)}'
                if application_dates_url:
                    application_dates_data = self.download(application_dates_url)
                    if application_dates_data:
                        application_data['dates_data'] = application_dates_data

                application_documents_url = get_application_href(application_soup, 'a[title="Link to View Related '
                                                                                   'Documents"]')
                if application_documents_url:
                    application_documents_page_data = self.download(application_documents_url)
                    if application_documents_page_data:
                        document_urls = self._get_document_url(application_documents_page_data)

                    if document_urls and isinstance(document_urls, list):
                        for document_url in document_urls:
                            # This is for cases when there are more than one document URLs.
                            document_data = self.download_document(document_url)
                            if document_data and isinstance(document_data, bytes):
                                break
                    else:
                        document_data = self.download_document(url)

                    if document_data:
                        application_data['document_data'] = document_data

            all_page_raw_data.append(application_data)

        return all_page_raw_data

    def _get_document_url(self, page_data: str):
        soup = BeautifulSoup(page_data, 'lxml')

        document_urls = None
        viewstate = None
        viewstate_generator = None
        event_validation = None
        event_target = None
        case_no = None

        viewstate_tag = soup.select_one('input#__VIEWSTATE')
        viewstate_generator_tag = soup.select_one('input#__VIEWSTATEGENERATOR')
        event_validation_tag = soup.select_one('input#__EVENTVALIDATION')
        application_form_tag = soup.select_one('span:-soup-contains("Application Form")')
        case_no_tag = soup.select_one('span#lblCaseNo')

        if not application_form_tag:
            logging.info('No document URL for this application.')
            return document_urls
        else:
            event_target_pattern = r'gvDocs\$ctl\d+\$lnkDShow'
            event_target_tag = application_form_tag.find_parent('tr').select_one('a')
            if event_target_tag and event_target_tag.has_attr('href'):
                event_target_match = re.search(event_target_pattern, event_target_tag['href'])
                if event_target_match:
                    event_target = event_target_match.group(0)

        if viewstate_tag and viewstate_tag.has_attr('value'):
            viewstate = viewstate_tag['value']

        if viewstate_generator_tag and viewstate_generator_tag.has_attr('value'):
            viewstate_generator = viewstate_generator_tag['value']

        if event_validation_tag and event_validation_tag.has_attr('value'):
            event_validation = event_validation_tag['value']

        if case_no_tag:
            case_no = case_no_tag.get_text()

        if event_target:
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

                if len(document_tags) > 1:
                    document_urls = [tag['href'] for tag in document_tags if tag and tag.has_attr('href')]
                else:
                    document_tag = document_tags[0]
                    if document_tag and document_tag.has_attr('href'):
                        document_urls = document_tag['href']

        return document_urls

    @staticmethod
    def _get_search_result_data(soup) -> list:
        search_results = []
        page_links = soup.select('td.TableData a.data_text')

        for link in page_links:
            search_results.append(clean_href(link["href"]))

        return search_results
