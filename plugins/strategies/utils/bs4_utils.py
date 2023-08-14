import re

from bs4 import BeautifulSoup


def get_application_href(soup: BeautifulSoup, bs_selector: str) -> str:
    """
    :param soup: BeautifulSoup object
    :param bs_selector: selector to be passed in a select_one() method
    :return: Returns the href value of the tag if it has one.
    """
    application_href = None
    application_tag = soup.select_one(bs_selector)

    if application_tag and application_tag.has_attr('href'):
        application_href = application_tag['href']

    return application_href


def clean_href(href: str) -> str:
    """
    :param href: href to be cleaned
    :return:
    """
    return re.sub(r'\s', '', href.replace(" ", "%20"))
