"""
scrapers/zonaprop_scraper.py
============================
Scrapes apartment-for-sale listings from zonaprop.com.ar.

Strategy
--------
1. Build the search URL for each neighbourhood.
2. Load the page with Playwright (the site is Next.js / client-rendered).
3. Try to parse the embedded ``__NEXT_DATA__`` JSON blob first — it is the
   most reliable source and avoids brittle CSS selectors.
4. If ``__NEXT_DATA__`` is absent or incomplete, fall back to DOM parsing with
   BeautifulSoup using ``data-qa`` attributes (which are more stable than
   class names on React sites).
5. Repeat for each pagination page up to ``max_pages``.
6. Normalise prices to USD and compute price/m².

URL patterns (as of early 2026)
--------------------------------
Search  : https://www.zonaprop.com.ar/departamentos-venta-{slug}.html
Page N  : https://www.zonaprop.com.ar/departamentos-venta-{slug}-pagina-{n}.html
"""

from __future__ import annotations

import json
import logging
import random
import re
from typing import Optional

from bs4 import BeautifulSoup

from models.listing import Listing
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

BASE_DOMAIN = "https://www.zonaprop.com.ar"

# ---------------------------------------------------------------------------
# Superficie homogénea — pesos por tipo de m² (estándar Tribunal de Tasaciones)
# Modificá estos valores para ajustar el cálculo de precio/m².
# ---------------------------------------------------------------------------
SURFACE_WEIGHT_COVERED    = 1.00   # m² cubiertos   (living, dormitorios, cocina)
SURFACE_WEIGHT_SEMICOV    = 0.50   # m² semicubiertos (balcón techado, galería)
SURFACE_WEIGHT_UNCOVERED  = 0.30   # m² descubiertos (terraza, patio sin techo)

# Neighbourhood name → URL slug mapping.
# Keys must match those used in neighborhood_prices.json.
NEIGHBORHOOD_SLUGS: dict[str, str] = {
    "Caballito":         "caballito",
    "Almagro":           "almagro",
    "Villa Crespo":      "villa-crespo",
    "Flores":            "flores",
    "Palermo":           "palermo",
    "Belgrano":          "belgrano",
    "Recoleta":          "recoleta",
    "Balvanera":         "balvanera",
    "San Telmo":         "san-telmo",
    "Barracas":          "barracas",
    "Villa Urquiza":     "villa-urquiza",
    "Coghlan":           "coghlan",
    "Nunez":             "nunez",
    "Saavedra":          "saavedra",
    "Paternal":          "paternal",
    "Villa del Parque":  "villa-del-parque",
    "Monte Castro":      "monte-castro",
    "Liniers":           "liniers",
    "Mataderos":         "mataderos",
    "Villa Lugano":      "villa-lugano",
}


def _build_url(slug: str, page: int) -> str:
    if page == 1:
        return f"{BASE_DOMAIN}/departamentos-venta-{slug}.html"
    return f"{BASE_DOMAIN}/departamentos-venta-{slug}-pagina-{page}.html"


# ---------------------------------------------------------------------------
# Price parsing helpers
# ---------------------------------------------------------------------------

def _parse_price(raw: str) -> tuple[float, str]:
    """
    Extract (amount, currency) from strings such as:
      'USD 85.000'  →  (85000.0, 'USD')
      'U$S 85.000'  →  (85000.0, 'USD')
      '$ 8.500.000' →  (8500000.0, 'ARS')
      'Consultar'   →  (0.0, 'UNKNOWN')

    Safety: extracts only the FIRST standalone price token (digits + separators)
    that immediately follows the currency marker, ignoring any trailing text
    (e.g. "hace 3 meses", "Expensas", secondary prices).
    """
    raw = raw.strip()
    raw_upper = raw.upper()

    if not raw or "CONSULTAR" in raw_upper or "PRICE_UNDEF" in raw_upper:
        return 0.0, "UNKNOWN"

    # Determine currency
    if "USD" in raw_upper or "U$S" in raw_upper or "US$" in raw_upper:
        currency = "USD"
    else:
        currency = "ARS"

    # Extract the FIRST numeric token (digits, dots, commas) that appears in the string.
    # This prevents runaway concatenation when the element contains extra text/numbers.
    match = re.search(r"[\d][\d.,]*", raw)
    if not match:
        return 0.0, currency

    digits = match.group(0)

    # Argentine convention: '.' = thousands separator, ',' = decimal
    # e.g. '1.500.000' or '85.000'
    if "," in digits:
        # Has explicit decimal comma → remove dots (thousands), replace comma with dot
        digits = digits.replace(".", "").replace(",", ".")
    else:
        # Dots only — all are thousands separators (no decimal in ARS RE prices)
        digits = digits.replace(".", "")

    try:
        amount = float(digits)
    except ValueError:
        amount = 0.0

    return amount, currency


def _parse_surface(raw: str) -> float:
    """Extract square metres from strings like '45 m²' or '45m2'."""
    match = re.search(r"(\d+(?:[.,]\d+)?)", raw)
    if not match:
        return 0.0
    return float(match.group(1).replace(",", "."))


def _parse_rooms(raw: str) -> int:
    """Extract room count from strings like '2 Amb.' or '3 ambientes'."""
    match = re.search(r"(\d+)", raw)
    return int(match.group(1)) if match else 0


# ---------------------------------------------------------------------------
# Parsing strategies
# ---------------------------------------------------------------------------

def _extract_from_next_data(next_data: dict, neighborhood: str) -> list[dict]:
    """
    Navigate the ``__NEXT_DATA__`` JSON tree to locate listing records.

    Zonaprop embeds the listing array at different paths across versions:
      props.pageProps.listPostings
      props.pageProps.initialState.listPostings
      props.pageProps.searchResult.postings
    This function tries all known paths and returns whatever it finds.
    """
    page_props = next_data.get("props", {}).get("pageProps", {})

    # Try common paths
    candidates: list[dict] = []
    for key_path in [
        ["listPostings"],
        ["initialState", "listPostings"],
        ["searchResult", "postings"],
        ["initialProps", "pageProps", "listPostings"],
    ]:
        node = page_props
        for key in key_path:
            node = node.get(key) if isinstance(node, dict) else None
            if node is None:
                break
        if isinstance(node, list):
            candidates = node
            break

    raw_listings: list[dict] = []
    for item in candidates:
        try:
            posting = item if "priceOperationTypes" in item else item.get("postingData", item)

            # --- Price ---
            price_raw = ""
            price_usd = 0.0
            for pop in posting.get("priceOperationTypes", []):
                for price_entry in pop.get("prices", []):
                    currency = price_entry.get("currency", "")
                    amount = float(price_entry.get("amount", 0) or 0)
                    if currency == "USD":
                        price_usd = amount
                        break
                    elif currency == "ARS":
                        price_raw = str(amount)
                if price_usd:
                    break

            # --- Surface & rooms ---
            surface_m2 = 0.0
            rooms = 0
            for attr in posting.get("mainFeatures", {}).values() if isinstance(posting.get("mainFeatures"), dict) else []:
                label = str(attr.get("label", "")).lower()
                val = str(attr.get("value", ""))
                if "superficie" in label or "m²" in label or "total" in label:
                    surface_m2 = _parse_surface(val)
                elif "amb" in label or "dorm" in label or "cuart" in label:
                    rooms = _parse_rooms(val)

            # Fallback: features list
            if surface_m2 == 0 or rooms == 0:
                for feat in posting.get("features", []):
                    label = str(feat.get("label", "")).lower()
                    val = str(feat.get("value", ""))
                    if surface_m2 == 0 and ("m²" in label or "sup" in label):
                        surface_m2 = _parse_surface(val)
                    if rooms == 0 and ("amb" in label or "dorm" in label):
                        rooms = _parse_rooms(val)

            # --- Location ---
            location = posting.get("postingLocation", {})
            barrio = (
                location.get("subdivisionName", {}).get("name", "")
                or location.get("location", {}).get("name", "")
                or neighborhood
            )

            # --- URL ---
            url = posting.get("url", "") or posting.get("shareUrl", "")
            if url and not url.startswith("http"):
                url = BASE_DOMAIN + url

            # --- Title ---
            title = (
                posting.get("title", "")
                or posting.get("description", "Departamento en venta")
            )

            raw_listings.append(
                {
                    "title":        title,
                    "price_usd":    price_usd,
                    "price_raw":    price_raw,
                    "surface_m2":   surface_m2,
                    "rooms":        rooms,
                    "neighborhood": barrio or neighborhood,
                    "url":          url,
                }
            )
        except Exception as exc:
            logger.debug("Skipping malformed __NEXT_DATA__ posting: %s", exc)

    return raw_listings


def _extract_from_dom(soup: BeautifulSoup, neighborhood: str) -> list[dict]:
    """
    DOM parser using the CSS Module class patterns Zonaprop uses (2025-2026).
    Primary card selector: .postingCardLayout-module__posting-card-layout
    Falls back to broader patterns if that changes.
    """
    raw_listings: list[dict] = []

    cards = (
        soup.select(".postingCardLayout-module__posting-card-layout")
        or soup.select("[data-qa='POSTING_CARD']")
        or soup.select(".postingCard, .posting-card-layout")
    )

    for card in cards:
        try:
            # --- Price ---
            # The price element may contain "USD 145.000$ 146.000 Expensas" — take only the first price
            price_el = card.select_one("[class*='price']")
            price_text = price_el.get_text(strip=True) if price_el else ""
            # Split on '$' or 'USD' repetitions and take the first token
            # e.g. "USD 145.000$ 146.000 Expensas" → "USD 145.000"
            price_first = re.split(r'(?<=\d)\s*\$', price_text)[0].strip()
            price_usd, currency = _parse_price(price_first)
            price_raw = price_first if currency == "ARS" else ""

            # --- Features (rooms, surface) ---
            feature_items = card.select("[class*='posting-main-features-span']")
            surface_cub  = 0.0   # m² cubiertos
            surface_semi = 0.0   # m² semicubiertos
            surface_desc = 0.0   # m² descubiertos
            surface_tot  = 0.0   # m² totales (fallback si no hay desglose)
            rooms = 0
            for feat in feature_items:
                text = feat.get_text(strip=True).lower()
                if "m²" in text or "m2" in text:
                    # Skip ranges like "51 a 752 m²" (emprendimientos)
                    if " a " not in text:
                        val = _parse_surface(text)
                        if "semi" in text or "sem." in text:
                            surface_semi = val
                        elif "desc" in text or "abierto" in text or "patio" in text or "terr" in text:
                            surface_desc = val
                        elif "cub" in text:
                            surface_cub = val
                        else:
                            # sin etiqueta → total (o cubiertos si es el único valor)
                            surface_tot = val
                elif "amb" in text:
                    # Skip ranges like "1 a 4 amb."
                    if " a " not in text:
                        rooms = _parse_rooms(text)

            # Superficie homogénea (Tribunal de Tasaciones de la Nación)
            if surface_cub > 0 or surface_semi > 0 or surface_desc > 0:
                surface_m2 = (
                    surface_cub  * SURFACE_WEIGHT_COVERED   +
                    surface_semi * SURFACE_WEIGHT_SEMICOV   +
                    surface_desc * SURFACE_WEIGHT_UNCOVERED
                )
            else:
                # Sin desglose: usar el total como cubiertos (conservador)
                surface_m2 = surface_tot

            # --- Location ---
            # Format examples:
            #   "Neuquen 800 Caballito, Capital Federal"  → barrio = "Caballito"
            #   "Nicolas Repetto al 1100 Caballito Norte, Caballito" → barrio = "Caballito"
            loc_el = card.select_one("[class*='location-block']")
            barrio = neighborhood
            if loc_el:
                loc_text = loc_el.get_text(" ", strip=True)
                parts = [p.strip() for p in loc_text.split(",")]
                if len(parts) >= 2:
                    # Last part before "Capital Federal" / "CABA" is the canonical barrio
                    candidate = parts[-2].strip() if len(parts) > 2 else parts[0].strip()
                    # Remove leading street address (anything up to and including the last digit)
                    candidate = re.sub(r'^.*\d+\s*', '', candidate).strip()
                    if candidate:
                        barrio = candidate
                if not barrio or barrio == neighborhood:
                    # Fallback: take whatever is before the first comma, strip the address
                    raw = parts[0] if parts else ""
                    raw = re.sub(r'^.*\d+\s*', '', raw).strip()
                    barrio = raw or neighborhood

            # --- URL ---
            link = card.find("a", href=True)
            url = ""
            if link:
                href = link.get("href", "")
                url = href if href.startswith("http") else BASE_DOMAIN + href

            # --- Title / description ---
            title_el = card.select_one(
                "[class*='description'], [class*='title'], h2, h3"
            )
            title = title_el.get_text(strip=True) if title_el else "Departamento en venta"

            raw_listings.append(
                {
                    "title":        title,
                    "price_usd":    price_usd,
                    "price_raw":    price_raw,
                    "surface_m2":   surface_m2,
                    "rooms":        rooms,
                    "neighborhood": barrio,
                    "url":          url,
                }
            )
        except Exception as exc:
            logger.debug("Skipping DOM card: %s", exc)

    return raw_listings


def _has_next_page(soup: BeautifulSoup) -> bool:
    """Return True if a 'next page' link is present in the pagination."""
    return bool(
        soup.select_one(
            "[data-qa='PAGING_NEXT'], "
            "a[aria-label='Siguiente'], "
            ".paging-next"
        )
    )


# ---------------------------------------------------------------------------
# Scraper class
# ---------------------------------------------------------------------------

class ZonapropScraper(BaseScraper):
    """Scrapes zonaprop.com.ar for apartment listings in Buenos Aires."""

    SOURCE = "zonaprop"

    def scrape(self) -> list[Listing]:
        listings: list[Listing] = []
        seen_urls: set[str] = set()

        self._start_browser()
        try:
            for idx, neighborhood in enumerate(self.neighborhoods):
                slug = NEIGHBORHOOD_SLUGS.get(neighborhood)
                if not slug:
                    logger.warning("No URL slug for neighbourhood '%s' — skipping.", neighborhood)
                    continue

                # Rotate browser context between neighbourhoods to reset Cloudflare
                # cookies/session fingerprint.  Keep the same Chromium process (fast)
                # but get a clean cookie jar and storage each time.
                if idx > 0:
                    import time as _time_delay
                    _time_delay.sleep(random.uniform(4, 8))
                    self._rotate_context()

                logger.info("[Zonaprop] Scraping neighbourhood: %s (slug=%s)", neighborhood, slug)

                for page_num in range(1, self.max_pages + 1):
                    url = _build_url(slug, page_num)
                    logger.debug("  → page %d: %s", page_num, url)

                    page = self._get_page(url)
                    if page is None:
                        logger.warning("  Could not load page %d for %s — stopping pagination.", page_num, neighborhood)
                        break

                    # Give JS-heavy pages a moment to finish rendering
                    import time as _time
                    _time.sleep(3)
                    html = page.content()
                    page.close()
                    soup = BeautifulSoup(html, "lxml")

                    # --- Strategy 1: __NEXT_DATA__ ---
                    raw: list[dict] = []
                    next_data_tag = soup.find("script", id="__NEXT_DATA__")
                    if next_data_tag and next_data_tag.string:
                        try:
                            next_data = json.loads(next_data_tag.string)
                            raw = _extract_from_next_data(next_data, neighborhood)
                            logger.debug("  __NEXT_DATA__: found %d postings", len(raw))
                        except json.JSONDecodeError as exc:
                            logger.debug("  __NEXT_DATA__ parse error: %s", exc)

                    # --- Strategy 2: DOM fallback ---
                    if not raw:
                        raw = _extract_from_dom(soup, neighborhood)
                        logger.debug("  DOM fallback: found %d cards", len(raw))

                    if not raw:
                        logger.info("  No listings on page %d — stopping pagination for %s.", page_num, neighborhood)
                        break

                    # --- Build Listing objects ---
                    for item in raw:
                        url_clean = item["url"].split("?")[0].rstrip("/")
                        if not url_clean or url_clean in seen_urls:
                            continue
                        seen_urls.add(url_clean)

                        price_usd = item["price_usd"]
                        if price_usd == 0 and item.get("price_raw"):
                            price_usd = self.currency.ars_to_usd(
                                _parse_price(item["price_raw"])[0]
                            )

                        if price_usd <= 0 or item["surface_m2"] <= 0:
                            continue

                        # Sanity caps — Buenos Aires apartments are never this expensive
                        MAX_PRICE_USD = 5_000_000      # $5M USD absolute ceiling
                        MAX_PRICE_M2  = 50_000         # $50k USD/m² absolute ceiling
                        price_m2_check = price_usd / item["surface_m2"]
                        if price_usd > MAX_PRICE_USD or price_m2_check > MAX_PRICE_M2:
                            logger.debug(
                                "Skipping implausible price: price_usd=%.0f price_m2=%.0f url=%s",
                                price_usd, price_m2_check, item.get("url", ""),
                            )
                            continue

                        listing = Listing(
                            source=self.SOURCE,
                            title=item["title"] or "Departamento en venta",
                            price_usd=price_usd,
                            surface_m2=item["surface_m2"],
                            rooms=item["rooms"],
                            neighborhood=item["neighborhood"],
                            url=url_clean,
                        )
                        if listing.is_valid():
                            listings.append(listing)

                    # Stop pagination if no next-page link
                    if not _has_next_page(soup):
                        logger.debug("  Last page reached for %s.", neighborhood)
                        break

        finally:
            self._stop_browser()

        logger.info("[Zonaprop] Total valid listings collected: %d", len(listings))
        return listings
