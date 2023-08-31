import base64
import io
import json
import logging
import re

from PyPDF2 import PdfReader

from scripts.base.parser import ParsingStrategy
from scripts.parser.defaults import Defaults


class AmbervalleyGovUkParsingStrategy(ParsingStrategy):
    def parse(self, data: dict) -> dict:
        parsed_data = {}
        excluded_keys = ['date8_week']
        try:
            if 'date_captured' in data:
                parsed_data['date_captured'] = data['date_captured']

            if 'application_details' in data:
                application_details = data['application_details']
                if application_details.get('data', None):
                    parsed_data['application_details_source'] = application_details['source']
                    for key, val in application_details['data'].items():
                        if val:
                            words = re.findall(r'[A-Za-z][a-z]*|[A-Z][a-z]*', key)
                            snake_case_key = '_'.join(word.lower() for word in words)

                            if snake_case_key in excluded_keys:
                                continue

                            parsed_data[snake_case_key] = re.sub(r'\s', ' ', val) \
                                if isinstance(val, str) else val

            if 'application_form_document' in data and data['application_form_document']:
                application_form_document = data['application_form_document']
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
