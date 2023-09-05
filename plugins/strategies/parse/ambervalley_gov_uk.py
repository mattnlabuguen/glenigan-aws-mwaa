import base64
import io
import json
import logging
import re

from PyPDF2 import PdfReader

from strategies.base.parse_strategy import ParseStrategy
from strategies.parse.defaults import Defaults


class AmbervalleyGovUkParseStrategy(ParseStrategy):
    def parse(self, raw_data: dict) -> dict:
        """
        Parse the data from the crawler.
        :param data: The data from the crawler.
        :return: A dictionary containing the parsed data.
        """
        parsed_data = {}
        try:
            if 'date_captured' in raw_data:
                parsed_data['date_captured'] = raw_data['date_captured']

            if 'application_details' in raw_data:
                application_details = raw_data['application_details']
                if application_details.get('data', None):
                    parsed_data['application_details_source'] = application_details['source']
                    for key, val in application_details['data'].items():
                        if not val or not isinstance(val, str):
                            continue

                        key = self.format_title(key)
                        if not key:
                            continue

                        value = self.clean_substring(val)
                        parsed_data[key] = value

            if 'application_form_document' in raw_data and raw_data['application_form_document']:
                application_form_document = raw_data['application_form_document']
                if application_form_document.get('data', None):
                    parsed_data['application_form_document_source'] = application_form_document['source']

                    document_data = base64.b64decode(application_form_document['data'])
                    document_byte_stream = io.BytesIO(document_data)
                    document = PdfReader(document_byte_stream)

                    document_text = ' '.join([page.extract_text() for page in document.pages]).strip()
                    document_text = re.sub(r'\s+', ' ', document_text)

                    if 'eastings' not in parsed_data:
                        parsed_data['easting'] = self._get_document_values(document_text,
                                                                           r'Easting \(x\) (\d+)Northing')

                    if 'northings' not in parsed_data:
                        parsed_data['northings'] = self._get_document_values(document_text,
                                                                             r"\(y\) (\d+)")

                    parsed_data['planning_portal_reference'] = self._get_document_values(document_text,
                                                                                         r"(PP-\d{7})")

        except json.decoder.JSONDecodeError as e:
            logging.error(f'parse() error: {str(e)}')
        except KeyError as e:
            logging.error(f'parse() error: {str(e)}')

        return parsed_data

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

    @staticmethod
    def format_title(key: str):
        """
        Format the title to snake case.
        :param key: The title to format.
        :return: The formatted title.
        """
        excluded_keys = ['date8_week']
        words = re.findall(r'[A-Za-z][a-z]*|[A-Z]+', key)
        snake_case_key = '_'.join(word.lower() for word in words)

        if snake_case_key in excluded_keys:
            return None

        return snake_case_key

    @staticmethod
    def clean_substring(substring):
        return re.sub(r'\s', ' ', substring)

