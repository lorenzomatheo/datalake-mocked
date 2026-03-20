import json
import random
import re
import time
from typing import Any

import ftfy
import requests
from bs4 import BeautifulSoup

from maggulake.utils.time import agora_em_sao_paulo

BASE_URL = "https://www.sara.com.br"
CDN_URL = "https://cdn.sara.com.br"
PRODUCTS_MAP_URL = f"{BASE_URL}/mapa-de-produtos"
LEAFLETS_BASE_URL = f"{CDN_URL}/leafletDocuments"
SKIP_EAN_PATTERNS = ["9999999999999", "0000000000000"]
RATE_LIMIT = 0.5
MAX_RETRIES = 3


def create_session() -> requests.Session:
    session = requests.Session()
    user_agent = (
        "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36"
    )
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    response = session.get(PRODUCTS_MAP_URL, timeout=30)
    response.raise_for_status()
    return session


def get_rsc_headers(referer: str = PRODUCTS_MAP_URL) -> dict[str, str]:
    return {
        "Accept": "*/*",
        "Rsc": "1",
        "Referer": referer,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }


def get_html_headers() -> dict[str, str]:
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }


def should_skip_ean(ean: str) -> bool:
    return any(pattern in ean for pattern in SKIP_EAN_PATTERNS)


def fix_encoding(text: str | None) -> str | None:
    if text is None:
        return None
    return ftfy.fix_text(text)


def build_leaflet_url(relative_path: str | None) -> str | None:
    if not relative_path:
        return None
    if relative_path.startswith("leafletDocuments/"):
        relative_path = relative_path.replace("leafletDocuments/", "")
    return f"{LEAFLETS_BASE_URL}/{relative_path}"


def get_total_pages(session: requests.Session) -> int:
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                f"{PRODUCTS_MAP_URL}/1", headers=get_html_headers(), timeout=30
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            pagination = soup.find_all("a", href=re.compile(r"/mapa-de-produtos/\d+"))
            if not pagination:
                return 1

            page_numbers = []
            for a in pagination:
                match = re.search(r"/mapa-de-produtos/(\d+)$", a["href"])
                if match:
                    page_numbers.append(int(match.group(1)))

            return max(page_numbers) if page_numbers else 1
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code >= 500:
                if attempt < MAX_RETRIES - 1:
                    wait_time = (2**attempt) + random.uniform(0, 1)
                    print(
                        f"Server error getting pages, retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    continue
            print(f"Error getting total pages (HTTP error): {e}")
            return 1
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = (2**attempt) + random.uniform(0, 1)
                print(f"Network error getting pages, retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            print(f"Error getting total pages (network error): {e}")
            return 1
    return 1


def extract_product_links(session: requests.Session, page_num: int) -> list[str]:
    url = f"{PRODUCTS_MAP_URL}/{page_num}"
    for attempt in range(MAX_RETRIES):
        time.sleep(RATE_LIMIT + random.uniform(0, 0.5))
        try:
            response = session.get(url, headers=get_html_headers(), timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a", class_="text-custom_cyan")

            product_urls = []
            for link in links:
                href = link.get("href")
                if href and "/produto/" in href:
                    if href.startswith("/"):
                        href = f"{BASE_URL}{href}"
                    ean_candidate = href.split("-")[-1]
                    if not should_skip_ean(ean_candidate):
                        product_urls.append(href)

            return product_urls
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code >= 500:
                if attempt < MAX_RETRIES - 1:
                    wait_time = (2**attempt) + random.uniform(0, 1)
                    print(
                        f"Server error on page {page_num}, "
                        f"retrying in {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    continue
            print(f"Error extracting links from page {page_num} (HTTP error): {e}")
            return []
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = (2**attempt) + random.uniform(0, 1)
                print(
                    f"Network error on page {page_num}, retrying in {wait_time:.1f}s..."
                )
                time.sleep(wait_time)
                continue
            print(f"Error extracting links from page {page_num} (network error): {e}")
            return []
    return []


def extract_json_from_rsc(response_text: str) -> dict[str, Any]:
    data = {}
    for line in response_text.split("\n"):
        if not line:
            continue

        match = re.match(r'^([a-f0-9]+):(.+)$', line)
        if match:
            key, value = match.groups()
            try:
                if value.startswith("{") or value.startswith("["):
                    data[key] = json.loads(value)
            except json.JSONDecodeError:
                pass
    return data


def _fetch_product_rsc(
    session: requests.Session, product_url: str
) -> requests.Response | None:
    rsc_url = f"{product_url}?_rsc=msw6x"
    for attempt in range(MAX_RETRIES):
        time.sleep(RATE_LIMIT + random.uniform(0, 0.5))
        try:
            response = session.get(
                rsc_url, headers=get_rsc_headers(PRODUCTS_MAP_URL), timeout=30
            )
            response.raise_for_status()
            return response
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code >= 500:
                if attempt < MAX_RETRIES - 1:
                    wait_time = (2**attempt) + random.uniform(0, 1)
                    print(
                        f"Server error {e.response.status_code} for {product_url}, "
                        f"retrying in {wait_time:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    time.sleep(wait_time)
                    continue
            print(f"Error parsing product {product_url} (HTTP error): {e}")
            return None
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = (2**attempt) + random.uniform(0, 1)
                print(
                    f"Network error for {product_url}, "
                    f"retrying in {wait_time:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(wait_time)
                continue
            print(f"Error parsing product {product_url} (network error): {e}")
            return None
    print(f"Failed to fetch {product_url} after {MAX_RETRIES} attempts")
    return None


def _find_presentation_data(rsc_data: dict[str, Any]) -> dict[str, Any]:
    presentation_data = rsc_data.get("c", {})
    if not presentation_data:
        for _, v in rsc_data.items():
            if isinstance(v, dict) and "drugUnit" in v and "ean1" in v:
                return v
    return presentation_data


def _find_product_data(rsc_data: dict[str, Any]) -> dict[str, Any]:
    product_data = rsc_data.get("1b", {})
    if not product_data:
        for _, v in rsc_data.items():
            if isinstance(v, dict) and "activeIngredient" in v and "productType" in v:
                return v
    return product_data


def _resolve_rsc_reference(val: Any, rsc_data: dict[str, Any]) -> Any:
    if isinstance(val, str) and val.startswith("$") and val[1:] in rsc_data:
        return rsc_data[val[1:]]
    return val


def _get_str_list(val: Any, rsc_data: dict[str, Any]) -> str | None:
    val = _resolve_rsc_reference(val, rsc_data)
    if isinstance(val, list):
        return ", ".join([str(v) for v in val])
    return str(val) if val is not None else None


def _build_product_result(
    presentation_data: dict[str, Any],
    product_data: dict[str, Any],
    rsc_data: dict[str, Any],
    product_url: str,
) -> dict[str, Any]:
    ean = (
        presentation_data.get("ean1")
        or product_data.get("ean")
        or product_url.split("-")[-1]
    )

    presentation_leaflet = _resolve_rsc_reference(
        presentation_data.get("presentationLeaflet", {}), rsc_data
    )
    marketing_company = _resolve_rsc_reference(
        presentation_data.get("marketingCompany", {}), rsc_data
    )
    company = _resolve_rsc_reference(product_data.get("company", {}), rsc_data)

    professional_leaflet_relative = (
        presentation_leaflet.get("professionalLeaflet")
        if isinstance(presentation_leaflet, dict)
        else None
    )
    patient_leaflet_relative = (
        presentation_leaflet.get("patientLeaflet")
        if isinstance(presentation_leaflet, dict)
        else None
    )

    return {
        "ean": ean,
        "product_name": fix_encoding(
            product_data.get("name") or presentation_data.get("presentationFriendly")
        ),
        "product_description": fix_encoding(presentation_data.get("descriptionSEO")),
        "anvisa_code": presentation_data.get("anvisaCode")
        or product_data.get("anvisaCode"),
        "active_ingredient": fix_encoding(
            _get_str_list(presentation_data.get("activeIngredients"), rsc_data)
            or product_data.get("activeIngredient")
        ),
        "drug_unit": fix_encoding(presentation_data.get("drugUnit")),
        "drug_quantity": fix_encoding(presentation_data.get("drugQuantity")),
        "dosage_form": fix_encoding(presentation_data.get("dosageForm")),
        "stripe": fix_encoding(presentation_data.get("stripe")),
        "regulatory_class": fix_encoding(product_data.get("regulatoryClass")),
        "therapeutical_class": fix_encoding(
            presentation_data.get("therapeuticalClass")
        ),
        "administration_routes": fix_encoding(
            _get_str_list(presentation_data.get("administrationRoutes"), rsc_data)
        ),
        "usage_restrictions": fix_encoding(
            _get_str_list(presentation_data.get("usageRestrictions"), rsc_data)
        ),
        "conservation": fix_encoding(
            _get_str_list(presentation_data.get("conservations"), rsc_data)
        ),
        "marketing_company": fix_encoding(
            marketing_company.get("name")
            if isinstance(marketing_company, dict)
            else None
        ),
        "manufacturer": fix_encoding(
            company.get("companyName") if isinstance(company, dict) else None
        ),
        "manufacturer_cnpj": (
            company.get("cnpj") if isinstance(company, dict) else None
        ),
        "professional_leaflet_url": build_leaflet_url(professional_leaflet_relative),
        "patient_leaflet_url": build_leaflet_url(patient_leaflet_relative),
        "product_url": product_url,
        "scraped_at": agora_em_sao_paulo(),
    }


def parse_product_data(
    session: requests.Session, product_url: str
) -> dict[str, Any] | None:
    response = _fetch_product_rsc(session, product_url)
    if response is None:
        return None

    try:
        rsc_data = extract_json_from_rsc(response.text)
        presentation_data = _find_presentation_data(rsc_data)
        product_data = _find_product_data(rsc_data)

        if not presentation_data and not product_data:
            print(f"Could not find product data in RSC response for {product_url}")
            return None

        return _build_product_result(
            presentation_data, product_data, rsc_data, product_url
        )
    except (KeyError, TypeError, AttributeError) as e:
        print(f"Error parsing product {product_url} (data extraction error): {e}")
        return None
