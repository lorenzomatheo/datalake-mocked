import re

import pandas as pd

# Clean whitespace and remove quotes/newlines


def clean_input(address: str) -> str:
    return address.strip().replace('\n', ' ').replace('"', '')


# Extract main address before slashes or hyphens


def get_main_segment(address: str) -> str:
    return re.split(r'\s*[\/-]\s*', address)[0]


# Remove various number markers

NUMBER_MARKERS = [
    r'\bN[°º]\s*',
    r'\bn[°º]\s*',
    r'\bNº\s*',
    r'\bNO\s*',
    r'\bn\.º\s*',
    r'\bn\.°\s*',
    r'\bnumero\s+',
    r'\bnúmero\s+',
]

NUMBER_MARKERS_COMPILED = [
    re.compile(pat, flags=re.IGNORECASE) for pat in NUMBER_MARKERS
]


def strip_number_markers(address: str) -> str:
    for patt in NUMBER_MARKERS_COMPILED:
        address = patt.sub(' ', address)
    return re.sub(r'\s+', ' ', address).strip()


# Parse date-based street names (e.g. Rua 27 de Setembro 258)

_DATE_STREET = re.compile(
    r'^((?:Rua|Avenida|Av)\s+\d+\s+de\s+[A-Za-zç]+)'  # street part
    r'(?:\s+(\d+[A-Za-z]?)(?:\s+.*)?)?$',
    flags=re.IGNORECASE,
)


def parse_date_street(addr: str) -> tuple[str, str] | None:
    m = _DATE_STREET.search(addr)
    if not m:
        return None
    street = m.group(1).strip()
    number = m.group(2).strip() if m.group(2) else None
    return street, number


# General parsing for street and number


def parse_general(addr: str) -> tuple[str, str] | tuple[str, None]:
    parts = [p.strip() for p in addr.split(',')]
    # Case 1: no comma
    if len(parts) == 1:
        m = re.search(r'^(.*?)\s+(\d+[A-Za-z]?)(?:\s+.*)?$', parts[0])
        if m and not re.search(
            r'\d+\s+de\s+[A-Za-zç]+$', m.group(1), flags=re.IGNORECASE
        ):
            return m.group(1).strip(), m.group(2).strip()
        return parts[0], None
    # Case 2: comma-separated
    street = parts[0]
    number = None
    # try second part
    if len(parts) > 1 and re.search(r'\d+', parts[1]):
        number = re.sub(r'[Nn]\.?[°º]?\s*', '', parts[1], flags=re.IGNORECASE).strip()
    # fallback: extract from street
    if not number:
        m2 = re.search(r'^(.*?)\s+(\d+[A-Za-z]?)(?:\s+.*)?$', street)
        if m2 and not re.search(
            r'\d+\s+de\s+[A-Za-zç]+$', m2.group(1), flags=re.IGNORECASE
        ):
            return m2.group(1).strip(), m2.group(2).strip()
    return street, number


# Standardize common abbreviations via mapping

ABBREVIATIONS = {
    r'\bR\.': 'Rua',
    r'\bAv\.': 'Avenida',
    r'\bRod\.': 'Rodovia',
    r'\bTrav\.': 'Travessa',
    r'\bPres\.?': 'Presidente',
    r'\bGov\.?': 'Governador',
}

ABBREV_COMPILED = [
    (re.compile(k, flags=re.IGNORECASE), v) for k, v in ABBREVIATIONS.items()
]


def standardize_abbrev(street: str) -> str:
    for patt, repl in ABBREV_COMPILED:
        street = patt.sub(repl + ' ', street)
    return re.sub(r'\s+', ' ', street).strip()


# Clean and truncate number


def clean_number(num: str) -> str | None:
    if not num:
        return None
    num = re.sub(r'[,\s]+$', '', num)
    m = re.match(r'^(\d+[A-Za-z]?)', num)
    return m.group(1) if m else None


# Main function


def normalize_address(address: str) -> str | None:
    if pd.isna(address):
        return None

    raw = clean_input(address)
    main = get_main_segment(raw)
    main = strip_number_markers(main)

    parsed = parse_date_street(main)
    if parsed:
        street, number = parsed
    else:
        street, number = parse_general(main)

    if street:
        street = standardize_abbrev(street)
    number = clean_number(number)

    if street and number:
        return f"{street}, {number}"
    return street or raw
