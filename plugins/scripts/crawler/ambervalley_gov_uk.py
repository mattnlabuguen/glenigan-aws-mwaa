import logging
import json
from datetime import datetime, timedelta

from scripts.base.crawler import CrawlingStrategy
from scripts.downloader.zyte_downloader import ZyteDownloader


class AmbervalleyGovUkCrawlingStrategy(CrawlingStrategy):
    def __init__(self):
        self.downloader = ZyteDownloader(country='uk')
        self.base_url = 'https://info.ambervalley.gov.uk/WebServices/AVBCFeeds'
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
        self.logging = Logger(self.__class__.__name__).logger

    def download(self, url, timeout=10000, headers=None, cookies=None, data=None, is_document=False):
        raw_data = None

        if not isinstance(headers, dict):
            headers = {
                "Host": "info.ambervalley.gov.uk",  # This is a required header
                "Origin": "https://www.ambervalley.gov.uk",
                "Referer": "https://www.ambervalley.gov.uk/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
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
            error_message = f'download() error: {str(e)}'
            self.logging.error(error_message)
            raise Exception(error_message)

        return raw_data

    def get_sources(self, months_ago: int = 1) -> list:
        self.logging.info('Getting reference numbers...')

        date_start = datetime.now() - timedelta(days=30 * months_ago)
        date_end = datetime.now()

        reference_numbers = []
        try:
            request_path = '/DevConJSON.asmx/PlanAppsByAddressKeyword'
            request_url = f'{self.base_url}{request_path}'

            from_date = date_start.strftime('%d/%b/%Y')
            to_date = date_end.strftime('%d/%b/%Y')

            form_data = f"keyWord=&fromDate={from_date}&toDate={to_date}"
            self.logging.info(f'Requesting data from {from_date} to {to_date} to {request_url}')
            search_data = self.download(request_url, headers=self.post_request_headers, timeout=300000, data=form_data)

            if search_data:
                json_data = json.loads(search_data)
                if json_data and isinstance(json_data, list):
                    reference_numbers = [data['refVal'] for data in json_data if 'refVal' in data and data['refVal']]
                    self.logging.info(f'Found {len(reference_numbers)} reference numbers')
            else:
                raise Exception('No data found')

        except Exception as e:
            error_message = f'get_sources() error: {str(e)}'
            self.logging.error(error_message)
            raise Exception(error_message)

        return reference_numbers

    def crawl(self, reference_numbers: list) -> list:
        self.logging.info('Getting data from each source...')
        raw_data_list = []
        for ref_val in reference_numbers:
            try:
                self.logging.info(f'Getting data for reference number: {ref_val}')
                planning_application_data = {
                    'application_details': self._get_planning_application_details(ref_val),
                    'application_form_document': self._get_planning_application_document(ref_val),
                    'date_captured': datetime.now().strftime('%Y-%m-%dT%H%M%S')
                }
                raw_data_list.append(planning_application_data)

            except Exception as e:
                error_message = f'crawl() error: {str(e)}'
                self.logging.error(error_message)
                raise Exception(error_message)

        return raw_data_list

    def _get_planning_application_details(self, ref_val: str) -> dict:
        planning_application_details = dict(data=None, source=None)
        request_path = '/DevConJSON.asmx/GetPlanAppDetails'
        request_url = f'{self.base_url}{request_path}?refVal={ref_val}'

        details_data = self.download(request_url)
        if details_data:
            try:
                json_data = json.loads(details_data)
                if isinstance(json_data, dict):
                    planning_application_details['data'] = json_data
                    planning_application_details['source'] = request_url
            except json.decoder.JSONDecodeError as e:
                self.logging.error(f'_get_planning_application_details() error: {str(e)}')

        return planning_application_details

    def _get_planning_application_document(self, ref_val: str) -> dict:
        planning_application_document = dict(data=None, source=None)
        request_path = '/IdoxEDMJSON.asmx/GetIdoxEDMDocListForCase'
        request_url = (f'{self.base_url}{request_path}?'
                       f'refVal={ref_val}&docApplication=planning')

        document_list_data = self.download(request_url)
        if document_list_data:
            try:
                document_id = None
                json_data = json.loads(document_list_data)
                if isinstance(json_data, list):
                    for document in json_data:
                        if 'docType' in document and document['docType'] == 'Application Form Redacted':
                            document_id = document['docId'] if 'docId' in document else None

                if document_id:
                    document_request_path = '/IdoxEDMJSON.asmx/StreamIdoxEDMDoc'
                    document_url = (f'{self.base_url}{document_request_path}?'
                                    f'docId={document_id}&docApplication=planning')

                    document_data = self.download(document_url, is_document=True)
                    if document_data:
                        planning_application_document['data'] = document_data
                        planning_application_document['source'] = document_url

            except json.decoder.JSONDecodeError as e:
                self.logging.error(f'_get_planning_application_document() error: {str(e)}')
        else:
            self.logging.info('No document found for this planning application.')

        return planning_application_document
