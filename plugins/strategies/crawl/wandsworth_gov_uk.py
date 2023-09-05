import base64
import logging
import re
from datetime import datetime
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

from strategies.base.crawl_strategy import CrawlStrategy
from strategies.downloader.zyte import ZyteDownloader
from utils.bs_utils import get_tag_attribute, get_aspnet_variables


class WandsworthGovUkCrawlStrategy(CrawlStrategy):
    def __init__(self):
        self.download = ZyteDownloader(country='uk').download
        self.base_application_url = 'https://planning.wandsworth.gov.uk/Northgate/PlanningExplorer/Generic/'
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

    def crawl(self, source: str) -> dict:
        """
        Crawls the planning application source/URL and returns all page data.
        :param source: the planning application source/URL
        :return: a dictionary containing all page data
        """
        logging.info('Getting all page data from each source...')
        try:
            logging.info(f'Page: {source}')
            planning_application_data = {
                'application_details': {
                    'main_page': {'data': str(), 'source': str()},
                    'dates_page': {'data': str(), 'source': str()},
                    'document_urls': {}
                },
                'application_form_document': None,
                'date_captured': datetime.now().strftime('%Y-%m-%dT%H%M%S')
            }

            main_page_data = self.download(source, headers=self.default_headers)
            if main_page_data:
                planning_application_data['application_details']['main_page'] = {
                    'data': main_page_data,
                    'source': source
                }
                main_page_soup = BeautifulSoup(main_page_data, 'lxml')

                planning_application_data['application_details']['dates_page'] = (
                    self._get_dates_page_data(main_page_soup)
                )

                documents_page_data = self._get_documents_page_data(main_page_soup)
                if documents_page_data:
                    all_documents_post_page_data = self._get_all_documents_post_page_data(documents_page_data)

                    for document_type, document_post_page_data in all_documents_post_page_data.items():
                        document_urls = self._get_document_urls(document_post_page_data)
                        logging.info(f'Found {len(document_urls)} {document_type} document/s')
                        planning_application_data['application_details']['document_urls'][document_type] = document_urls

                        if document_type == 'application_form':
                            planning_application_data['application_form_document'] = (
                                self._get_application_form_document_data(document_urls)
                            )
            else:
                raise Exception('Failed to get main page data')

        except Exception as e:
            error_message = f'crawl() error: {str(e)}'
            logging.error(error_message)
            planning_application_data = {
                'error': str(e),
                'application_form_url': source,
                'date_captured': datetime.now().strftime('%Y-%m-%dT%H%M%S')
            }

        return planning_application_data

    def _get_dates_page_data(self, soup: BeautifulSoup):
        """
        Gets the dates page data.
        :param soup: the main page soup
        :return: a dictionary containing the dates page data and source
        """
        dates_page = dict(data=str(), source=str())
        dates_page_url = None

        dates_page_tag = soup.select_one('a[title="Link to the application Dates page."]')
        href = get_tag_attribute(dates_page_tag, 'href')

        if href:
            href = re.sub(r'\s', '', href.replace(' ', '%20'))
            dates_page_url = f'{self.base_application_url}{href}'

        if dates_page_url:
            dates_page['data'] = self.download(dates_page_url, headers=self.default_headers)
            dates_page['source'] = dates_page_url

        return dates_page

    def _get_documents_page_data(self, soup: BeautifulSoup):
        """
        Gets the documents page data.
        :param soup: the main page soup
        :return: the documents page data
        """
        documents_page_data = None
        documents_page_url_tag = soup.select_one('a[title="Link to View Related Documents"]')
        documents_page_url = get_tag_attribute(documents_page_url_tag, 'href')
        if documents_page_url:
            documents_page_data = self.download(documents_page_url, headers=self.default_headers)

        return documents_page_data

    def _get_all_documents_post_page_data(self, page_data: str) -> dict:
        """
        Gets all documents post page data.
        :param page_data: the documents page data
        :return: a dictionary containing all documents post page data
        """
        all_documents_post_page_data = {}
        document_event_targets = {}
        case_no = None

        soup = BeautifulSoup(page_data, 'lxml')

        viewstate, viewstate_generator, event_validation = get_aspnet_variables(soup)
        case_no_tag = soup.select_one('span#lblCaseNo')
        document_type_tags = soup.select('span[id*="lblChoice"]')

        if document_type_tags:
            document_event_targets = {}
            for tag in document_type_tags:
                document_type_title = f"{tag.get_text().strip().replace(' ', '_').lower()}"
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
            logging.info(f'Found {len(document_event_targets)} document type/s for {case_no}')
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

                logging.info(f'Getting post page data for {document_title} documents...')
                post_page_data = self.download(page_url, headers=self.post_request_headers, data=form_data)
                all_documents_post_page_data.update({document_title: post_page_data})
        else:
            logging.info('No documents for this planning application')

        return all_documents_post_page_data

    def _get_application_form_document_data(self, application_form_urls: list):
        """
        Gets the application form document data.
        :param application_form_urls: a list of application form URLs
        :return: a dictionary containing the application form document data and source
        """
        application_form_document = {
            'data': base64.b64encode(b'').decode('utf-8'),
            'source': str()
        }

        for url in application_form_urls:
            document = self.download(url, is_document=True)
            if document is not b'' and isinstance(document, bytes):
                encoded_data = base64.b64encode(document).decode('utf-8')
                application_form_document['data'] = encoded_data
                application_form_document['source'] = url
                # Take the first instance of returned data that is in bytes.
                break

        return application_form_document

    @staticmethod
    def _get_document_urls(document_post_page_data: str):
        """
        Gets the document URLs.
        :param document_post_page_data: the document post page data
        :return: a list of document URLs
        """
        soup = BeautifulSoup(document_post_page_data, 'lxml')
        document_urls = []
        url_tags = soup.select('a[target="_blank"]')
        for tag in url_tags:
            href = get_tag_attribute(tag, 'href')
            if href:
                document_urls.append(href)

        return document_urls
