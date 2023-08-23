import copy
import io
import logging
import re

from bs4 import BeautifulSoup
from PyPDF2 import PdfReader

from scripts.base.parser import ParsingStrategy
from scripts.parser.defaults import Defaults


class WandsworthGovUkParsingStrategy(ParsingStrategy):
    def parse(self, raw_data: dict):
        data = {}
        try:
            main_details_soup = None
            dates_soup = None
            document = None

            if 'main_details_data' in raw_data and raw_data['main_details_data']:
                main_details_soup = BeautifulSoup(raw_data['main_details_data'], 'lxml')
                application_number = None if not main_details_soup \
                    else self.get_table_value(main_details_soup, 'Application Number')
                if application_number:
                    logging.info(f'Parsing through Application Number: {application_number}')
                else:
                    raise

            if 'dates_data' in raw_data and raw_data['dates_data']:
                dates_soup = BeautifulSoup(raw_data['dates_data'], 'lxml')

            if 'document_data' in raw_data and raw_data['document_data']:
                document_byte_stream = io.BytesIO(raw_data['document_data'])
                document = PdfReader(document_byte_stream)

            if 'source' in raw_data and raw_data['source']:
                data['source'] = raw_data['source']

            if main_details_soup:
                main_field_tags = main_details_soup.select('div > span')
                if main_field_tags:
                    main_field_titles = [tag.get_text().strip() for tag in main_field_tags if tag.get_text().strip()]
                    for title in main_field_titles:
                        key = self.format_title(title)
                        raw_value = self.get_table_value(main_details_soup, title)
                        value = self.clean_substring(raw_value)

                        data.update({key:value})

            if dates_soup:
                date_field_tags = dates_soup.select('div > span')
                if date_field_tags:
                    date_field_tags = [tag.get_text().strip() for tag in date_field_tags if tag.get_text().strip()]
                    data.update({title.replace(' ', '_').lower(): self.get_table_value(dates_soup, title)
                                 for title in date_field_tags})

            if document:
                data['easting'] = self.get_document_values(document, r'Easting \(x\) (\d+)Northing')
                data['northing'] = self.get_document_values(document, r"\(y\) (\d+)")
                data['planning_portal_reference'] = self.get_document_values(document, r"(PP-\d{7})")

        except Exception as e:
            logging.error(f'parse() error: {str(e)}')
        finally:
            return data

    @staticmethod
    def get_document_values(document, pattern: str) -> str:
        value = Defaults.NOT_FOUND.value
        try:
            page_text = ' '.join([page.extract_text() for page in document.pages]).strip()
            page_text = re.sub(r'\s+', ' ', page_text)

            matches = list(re.finditer(pattern, page_text))
            if matches:
                value = ' '.join(set([match.group(1) for match in matches]))

        except Exception as e:
            logging.error(f'get_document_values() error: {str(e)}')
            value = Defaults.EXTRACTION_ERROR.value

        return value

    @staticmethod
    def get_table_value(soup, column_name: str) -> str:
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
            logging.error(f'get_table_values() error: {str(e)}')
            value = Defaults.EXTRACTION_ERROR.value

        return value

    def get_decision_values(self, soup):
        decision_text = Defaults.NOT_FOUND.value
        decision_date = Defaults.NOT_FOUND.value

        try:
            extracted_value = self.get_table_value(soup, 'Decision')
            if extracted_value not in [Defaults.NOT_FOUND.value, Defaults.EXTRACTION_ERROR.value]:
                cleaned_string = re.sub(r'\s+', ' ', extracted_value)
                date_pattern = r'\d{2}/\d{2}/\d{4}'
                date_match = re.search(date_pattern, cleaned_string)

                if date_match:
                    decision_date = date_match.group()

                decision_text = re.sub(date_pattern, '', cleaned_string).strip()

        except Exception as e:
            logging.error(str(e))

        return decision_text, decision_date

    @staticmethod
    def format_title(title: str):
        filtered_string = ''.join([char for char in title if char.isalpha()])
        formatted_title = filtered_string.strip().replace(' ', '_')

        return formatted_title

    @staticmethod
    def clean_substring(substring):
        return re.sub(r'\s', substring, ' ')
