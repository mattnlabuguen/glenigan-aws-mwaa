from bs4 import BeautifulSoup


def get_tag_attribute(tag, attribute: str):
    if tag and tag.has_attr(attribute):
        return tag[attribute]
    else:
        return None


def get_aspnet_variables(soup: BeautifulSoup) -> tuple:
    viewstate = None
    viewstate_generator = None
    event_validation = None

    if soup:
        viewstate_tag = soup.select_one('input#__VIEWSTATE')
        viewstate_generator_tag = soup.select_one('input#__VIEWSTATEGENERATOR')
        event_validation_tag = soup.select_one('input#__EVENTVALIDATION')

        if viewstate_tag:
            viewstate = get_tag_attribute(viewstate_tag, 'value')

        if viewstate_generator_tag:
            viewstate_generator = get_tag_attribute(viewstate_generator_tag, 'value')

        if event_validation_tag:
            event_validation = get_tag_attribute(event_validation_tag, 'value')

    return viewstate, viewstate_generator, event_validation
