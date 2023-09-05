import base64
import copy
import io
import logging
import re

from bs4 import BeautifulSoup
from PyPDF2 import PdfReader

from strategies.base.parse_strategy import ParseStrategy
from strategies.parse.defaults import Defaults


class WandsworthGovUkParseStrategy(ParseStrategy):
    def parse(self, raw_data: dict) -> dict:
        """
        Parses raw data into a dictionary of key-value pairs
        :param raw_data: raw data to be parsed
        :return: dictionary of key-value pairs
        """
        data = {}
        try:
            main_details_soup = None
            dates_soup = None
            document = None

            if 'error' in raw_data:
                return data

            application_details = raw_data.get('application_details', {})
            application_form_document = raw_data.get('application_form_document', {})

            if application_details:
                if 'main_page' in application_details and application_details['main_page']:
                    main_page = raw_data['application_details']['main_page']
                    if 'data' in main_page:
                        main_details_soup = BeautifulSoup(main_page['data'], 'lxml')

                    if 'source' in main_page:
                        data['main_page_url'] = main_page['source']

                if 'dates_page' in application_details and application_details['dates_page']:
                    dates_page = raw_data['application_details']['dates_page']
                    if 'data' in dates_page:
                        dates_soup = BeautifulSoup(dates_page['data'], 'lxml')

                    if 'source' in dates_page:
                        data['dates_page_url'] = dates_page['source']

                if 'document_urls' in application_details:
                    for document_type, urls in application_details['document_urls'].items():
                        data[f'{document_type}_urls'] = urls

            if application_form_document:
                if 'data' in application_form_document:
                    data = application_form_document['data']
                    decoded_data = base64.b64decode(data.encode('utf-8'))
                    document_byte_stream = io.BytesIO(decoded_data)
                    document = PdfReader(document_byte_stream)

                if 'source' in application_form_document:
                    data['application_form_url'] = application_form_document['source']

            if main_details_soup:
                main_field_tags = main_details_soup.select('div > span')
                if main_field_tags:
                    main_field_titles = [tag.get_text().strip() for tag in main_field_tags if tag.get_text().strip()]
                    for title in main_field_titles:
                        key = self.format_title(title)
                        raw_value = self._get_table_value(main_details_soup, title)
                        value = self.clean_substring(raw_value)

                        data.update({key: value})

            if dates_soup:
                date_field_tags = dates_soup.select('div > span')
                if date_field_tags:
                    date_field_titles = [tag.get_text().strip() for tag in date_field_tags if tag.get_text().strip()]
                    for title in date_field_titles:
                        key = self.format_title(title)
                        raw_value = self._get_table_value(dates_soup, title)
                        value = self.clean_substring(raw_value)

                        data.update({key: value})

            if document:
                data['easting'] = self._get_document_values(document, r'Easting \(x\) (\d+)Northing')
                data['northing'] = self._get_document_values(document, r"\(y\) (\d+)")
                data['planning_portal_reference'] = self._get_document_values(document, r"(PP-\d{7})")

        except Exception as e:
            error_message = f'parse() error: {str(e)}'
            logging.error(error_message)

        return data

    @staticmethod
    def _get_document_values(document, pattern: str) -> str:
        value = Defaults.NOT_FOUND.value
        try:
            page_text = ' '.join([page.extract_text() for page in document.pages]).strip()
            page_text = re.sub(r'\s+', ' ', page_text)

            matches = list(re.finditer(pattern, page_text))
            if matches:
                value = ' '.join(set([match.group(1) for match in matches]))

        except Exception as e:
            logging.error(f'_get_document_values() error: {str(e)}')
            value = Defaults.EXTRACTION_ERROR.value

        return value

    @staticmethod
    def _get_table_value(soup, column_name: str) -> str:
        value = Defaults.NOT_FOUND.value
        soup_copy = copy.deepcopy(soup)
        try:
            pattern = re.compile(f'^' + re.escape(column_name) + '$')
            child_tag = soup_copy.find('span', text=pattern)
            if child_tag:
                parent_tag = child_tag.parent
                child_tag.decompose()

                tag_text = parent_tag.get_text().strip()

                if tag_text:
                    value = tag_text

        except Exception as e:
            logging.error(f'_get_table_values() error: {str(e)}')
            value = Defaults.EXTRACTION_ERROR.value

        return value

    @staticmethod
    def format_title(title: str):
        cleaned_title = re.sub(r'[^a-zA-Z0-9\s]', '', title)
        formatted_title = cleaned_title.lower().replace(" ", "_")
        return formatted_title

    @staticmethod
    def clean_substring(substring):
        return re.sub(r'\s', substring, ' ')
