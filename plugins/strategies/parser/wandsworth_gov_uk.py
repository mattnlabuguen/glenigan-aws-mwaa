import copy
import io
import logging
import re

from bs4 import BeautifulSoup
from PyPDF2 import PdfReader

from .parser import ParsingStrategy
from .defaults import Defaults


class WandsworthGovUkParsingStrategy(ParsingStrategy):
    def __init__(self):
        self.data_template = {
            'council_decision': Defaults.NOT_FOUND.value, 'application_number': Defaults.NOT_FOUND.value,
            'application_type': Defaults.NOT_FOUND.value, 'site_address': Defaults.NOT_FOUND.value,
            'proposal': Defaults.NOT_FOUND.value, 'appeal_submitted': Defaults.NOT_FOUND.value,
            'appeal_decision': Defaults.NOT_FOUND.value, 'appeal_date_lodged': Defaults.NOT_FOUND.value,
            'appeal_decision_date': Defaults.NOT_FOUND.value, 'received': Defaults.NOT_FOUND.value,
            'registered': Defaults.NOT_FOUND.value, 'decision_expiry': Defaults.NOT_FOUND.value,
            'easting': Defaults.NOT_FOUND.value, 'northing': Defaults.NOT_FOUND.value,
            'planning_portal_reference': Defaults.NOT_FOUND.value, 'source': None
        }

    def parse(self, raw_data_list: list):
        data_list = []
        for raw_data in raw_data_list:
            data = copy.deepcopy(self.data_template)

            main_details_soup = None
            dates_soup = None
            document = None
            application_number = None

            if 'main_details_data' in raw_data and raw_data['main_details_data']:
                main_details_soup = BeautifulSoup(raw_data['main_details_data'], 'lxml')
                application_number = None if not main_details_soup \
                    else self.get_table_value(main_details_soup, 'Application Number')

                # Uncomment for testing specific application numbers
                # if application_number not in ['2023/2441']:
                #     logging.info(f'Skipping Application Number: {application_number}')
                #     continue

                logging.info(f'Parsing through Application Number: {application_number}')

            if 'dates_data' in raw_data and raw_data['dates_data']:
                dates_soup = BeautifulSoup(raw_data['dates_data'], 'lxml')

            if 'document_data' in raw_data and raw_data['document_data']:
                document_byte_stream = io.BytesIO(raw_data['document_data'])
                document = PdfReader(document_byte_stream)

            if 'source' in raw_data and raw_data['source']:
                data['source'] = raw_data['source']

            if main_details_soup:
                data['application_number'] = application_number
                data['appeal_decision'], data['appeal_decision_date'] = self.get_decision_values(main_details_soup)
                data['council_decision'] = f"{data['appeal_decision']} {data['appeal_decision_date']}"

                data['application_type'] = self.get_table_value(main_details_soup, 'Application Type')
                data['site_address'] = self.get_table_value(main_details_soup, 'Site Address')
                data['proposal'] = self.get_table_value(main_details_soup, 'Proposal')
                data['appeal_submitted'] = self.get_table_value(main_details_soup, 'Appeal Submitted?')
                data['appeal_date_lodged'] = self.get_table_value(main_details_soup, 'Appeal Lodged')

            if dates_soup:
                data['received'] = self.get_table_value(dates_soup, 'Received?')
                data['registered'] = self.get_table_value(dates_soup, 'Registered')
                data['decision_expiry'] = self.get_table_value(dates_soup, 'Decision Expiry')

            if document:
                data['easting'] = self.get_document_values(document, r'Easting \(x\) (\d+)Northing')
                data['northing'] = self.get_document_values(document, r"\(y\) (\d+)")
                data['planning_portal_reference'] = self.get_document_values(document, r"(PP-\d{7})")

            data_list.append(data)

        return data_list

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
