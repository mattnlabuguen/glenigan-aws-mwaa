import re

from bs4 import BeautifulSoup


def get_href(soup: BeautifulSoup, bs_selector: str) -> str:
    """
    :param soup: BeautifulSoup object
    :param bs_selector: selector to be passed in a select_one() method
    :return: Returns the href value of the tag if it has one.
    """
    href = None
    tag = soup.select_one(bs_selector)

    if tag and tag.has_attr('href'):
        href = tag['href']

    return href


def clean_href(href: str) -> str:
    """
    :param href: href to be cleaned
    :return:
    """
    return re.sub(r'\s', '', href.replace(" ", "%20"))
