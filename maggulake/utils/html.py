import html2text


def clean_html(html_content: str) -> str:
    """
    Converts HTML content to plain text.
    """
    h = html2text.HTML2Text()
    h.ignore_links = True
    return h.handle(html_content)
