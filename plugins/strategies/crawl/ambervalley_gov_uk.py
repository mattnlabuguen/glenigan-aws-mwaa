import base64
import logging
import json
from datetime import datetime

from strategies.base.crawl_strategy import CrawlStrategy
from strategies.downloader.zyte import ZyteDownloader


class AmbervalleyGovUkCrawlStrategy(CrawlStrategy):
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

    def crawl(self, source: str) -> dict:
        """
        Get the data for the planning application.
        :param source: The reference number for the planning application.
        :return: A dictionary containing the data for the planning application.
        """
        logging.info(f'Getting data for reference number: {source}')
        try:
            planning_application_data = {
                'application_details': self._get_planning_application_details(source),
                'application_form_document': self._get_planning_application_document(source),
                'date_captured': datetime.now().strftime('%Y-%m-%dT%H%M%S')
            }
            raw_data = planning_application_data

        except Exception as e:
            error_message = f'crawl() error: {str(e)}'
            logging.error(error_message)
            raise Exception(error_message)

        return raw_data

    def _get_planning_application_details(self, ref_val: str) -> dict:
        """
        Get the planning application details.
        :param ref_val: The reference number for the planning application.
        :return: A dictionary containing the planning application details.
        """
        planning_application_details = {
            'data': dict(),
            'source': str()
        }
        request_path = '/DevConJSON.asmx/GetPlanAppDetails'
        request_url = f'{self.base_url}{request_path}?refVal={ref_val}'

        details_data = self.download(request_url, headers=self.default_headers)
        if details_data:
            try:
                json_data = json.loads(details_data)
                if isinstance(json_data, dict):
                    planning_application_details['data'] = json_data
                    planning_application_details['source'] = request_url
            except json.decoder.JSONDecodeError as e:
                logging.error(f'_get_planning_application_details() error: {str(e)}')

        return planning_application_details

    def _get_planning_application_document(self, ref_val: str) -> dict:
        """
        Get the planning application document.
        :param ref_val: The reference number for the planning application.
        :return: A dictionary containing the planning application document.
        """
        planning_application_document = {
            'data': str(),
            'source': str()
        }
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
                        encoded_document = base64.b64encode(document_data).decode('utf-8')
                        planning_application_document['data'] = encoded_document
                        planning_application_document['source'] = document_url

            except json.decoder.JSONDecodeError as e:
                logging.error(f'_get_planning_application_document() error: {str(e)}')
        else:
            logging.info('No document found for this planning application.')

        return planning_application_document
