import base64
import io
import logging
import re

from bs4 import BeautifulSoup
from PyPDF2 import PdfReader

from strategies.base.parse_strategy import ParseStrategy
from strategies.parse.defaults import Defaults


class HillingdonGovUkParseStrategy(ParseStrategy):
    def parse(self, raw_data):
        parsed_data = {}
        application_details = raw_data['application_details']

        if 'date_captured' in raw_data:
            parsed_data['date_captured'] = raw_data['date_captured']

        if application_details.get('data', None) and application_details.get('source', None):
            parsed_data['application_details_source'] = application_details['source']
            parsed_data.update(self._get_application_details(application_details['data']))

        application_form_document = raw_data['application_form_document']
        if application_form_document.get('data', None) and application_form_document.get('source', None):
            parsed_data['application_form_document_source'] = raw_data['application_form_document']['source']
            parsed_data.update(self._get_application_form_document(raw_data['application_form_document']['data']))

        return parsed_data

    @staticmethod
    def _get_application_details(data: str) -> dict:
        application_details = {}
        soup = BeautifulSoup(data, 'lxml')
        key_tags = soup.select('td:has(strong)')
        for key in key_tags:
            row_tag = key.parent
            value_tag = row_tag.select_one('td:last-child')
            if value_tag:
                value = re.sub(r'\s', ' ', value_tag.text).strip()
                words = re.findall(r'[A-Za-z][a-z]*|[A-Z][a-z]*', key.text)
                formatted_key = '_'.join(word.lower() for word in words)

                application_details[formatted_key] = value

        return application_details

    def _get_application_form_document(self, data: bytes) -> dict:
        application_form_document = {}
        try:
            decoded_data = base64.b64decode(data.encode('utf-8'))
            document = PdfReader(io.BytesIO(decoded_data))
            document_text = ' '.join([page.extract_text() for page in document.pages]).strip()
            document_text = re.sub(r'\s+', ' ', document_text)
            application_form_document['easting'] = self._get_document_values(document_text,
                                                                             r'Easting \(x\) (\d+)Northing')

            application_form_document['northing'] = self._get_document_values(document_text,
                                                                              r"\(y\) (\d+)")

            application_form_document['planning_portal_reference'] = self._get_document_values(document_text,
                                                                                               r"(PP-\d{7})")

        except Exception as e:
            logging.error(f'_get_application_form_document() error: {str(e)}')

        return application_form_document

    @staticmethod
    def _get_document_values(document_text, pattern: str) -> str:
        value = Defaults.NOT_FOUND.value
        try:
            matches = list(re.finditer(pattern, document_text))
            if matches:
                value = ' '.join(set([match.group(1) for match in matches]))

        except Exception as e:
            logging.error(f'_get_document_values() error: {str(e)}')
            value = Defaults.EXTRACTION_ERROR.value

        return value
