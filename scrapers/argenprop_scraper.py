"""
scrapers/argenprop_scraper.py
=============================
Scrapes apartment-for-sale listings from argenprop.com.

Strategy
--------
Same two-pass approach as the Zonaprop scraper:
  1. Parse the ``__NEXT_DATA__`` JSON blob embedded by Next.js.
  2. Fall back to DOM parsing using stable class-name and attribute selectors.

URL patterns (as of early 2026)
--------------------------------
Search  : https://www.argenprop.com/departamento-en-venta--en-{slug}
Page N  : https://www.argenprop.com/departamento-en-venta--en-{slug}?pagina={n}
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

from bs4 import BeautifulSoup

from models.listing import Listing
from scrapers.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

BASE_DOMAIN = "https://www.argenprop.com"

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
    base = f"{BASE_DOMAIN}/departamento-en-venta--en-{slug}"
    return base if page == 1 else f"{base}?pagina={page}"


# ---------------------------------------------------------------------------
# Price parsing helpers (same logic as Zonaprop, kept local for independence)
# ---------------------------------------------------------------------------

def _parse_price(raw: str) -> tuple[float, str]:
    """Return (amount_float, 'USD'|'ARS'|'UNKNOWN')."""
    raw = raw.strip()
    raw_upper = raw.upper()

    if not raw or "CONSULTAR" in raw_upper:
        return 0.0, "UNKNOWN"

    if "U$S" in raw_upper or "USD" in raw_upper or "US$" in raw_upper or "U$" in raw_upper:
        currency = "USD"
    else:
        currency = "ARS"

    digits = re.sub(r"[^\d.,]", "", raw)

    if "," in digits:
        digits = digits.replace(".", "").replace(",", ".")
    else:
        digits = digits.replace(".", "")

    try:
        amount = float(digits)
    except ValueError:
        amount = 0.0

    return amount, currency


def _parse_surface(text: str) -> float:
    match = re.search(r"(\d+(?:[.,]\d+)?)", text)
    return float(match.group(1).replace(",", ".")) if match else 0.0


def _parse_rooms(text: str) -> int:
    match = re.search(r"(\d+)", text)
    return int(match.group(1)) if match else 0


# ---------------------------------------------------------------------------
# Parsing strategies
# ---------------------------------------------------------------------------

def _extract_from_next_data(next_data: dict, neighborhood: str) -> list[dict]:
    """
    Walk the __NEXT_DATA__ tree from Argenprop.

    Known paths (may change with Next.js deployments):
      props.pageProps.listings
      props.pageProps.data.listings
      props.pageProps.initialState.listings
    """
    page_props = next_data.get("props", {}).get("pageProps", {})

    candidates: list = []
    for path in [
        ["listings"],
        ["data", "listings"],
        ["initialState", "listings"],
        ["listingsResult", "listings"],
    ]:
        node = page_props
        for key in path:
            node = node.get(key) if isinstance(node, dict) else None
            if node is None:
                break
        if isinstance(node, list) and node:
            candidates = node
            break

    raw_listings: list[dict] = []
    for item in candidates:
        try:
            # --- Price ---
            price_usd = 0.0
            price_raw = ""
            for field_name in ("price", "priceInDollars", "precioDolares"):
                val = item.get(field_name)
                if val is not None:
                    price_usd = float(val)
                    break
            if price_usd == 0:
                price_raw_str = str(item.get("priceText", "") or item.get("precio", "") or "")
                amount, cur = _parse_price(price_raw_str)
                if cur == "USD":
                    price_usd = amount
                elif cur == "ARS":
                    price_raw = price_raw_str

            # --- Surface ---
            surface_m2 = float(
                item.get("totalArea", 0)
                or item.get("coveredArea", 0)
                or item.get("superficieTotal", 0)
                or 0
            )
            if surface_m2 == 0:
                for feat in item.get("features", []):
                    label = str(feat.get("label", "")).lower()
                    val = str(feat.get("value", ""))
                    if "m²" in label or "sup" in label:
                        surface_m2 = _parse_surface(val)
                        break

            # --- Rooms ---
            rooms = int(item.get("rooms", 0) or item.get("ambientes", 0) or 0)
            if rooms == 0:
                for feat in item.get("features", []):
                    label = str(feat.get("label", "")).lower()
                    val = str(feat.get("value", ""))
                    if "amb" in label or "dorm" in label:
                        rooms = _parse_rooms(val)
                        break

            # --- Location ---
            barrio = (
                item.get("neighborhood", "")
                or item.get("barrio", "")
                or item.get("location", {}).get("neighborhood", "")
                or neighborhood
            )

            # --- URL ---
            url = item.get("url", "") or item.get("permalink", "")
            if url and not url.startswith("http"):
                url = BASE_DOMAIN + url

            # --- Title ---
            title = item.get("title", "") or item.get("titulo", "") or "Departamento en venta"

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
            logger.debug("Skipping malformed Argenprop __NEXT_DATA__ item: %s", exc)

    return raw_listings


def _extract_from_dom(soup: BeautifulSoup, neighborhood: str) -> list[dict]:
    """
    DOM fallback for Argenprop.
    The site has a card-based layout with predictable class names.
    """
    raw_listings: list[dict] = []

    # Try multiple known card selectors
    cards = (
        soup.select(".listing__item")
        or soup.select("[data-qa='posting']")
        or soup.select(".card-container")
    )

    for card in cards:
        try:
            # --- Price ---
            price_el = card.select_one(
                ".card__price, .price, [data-qa='price'], .listing-price"
            )
            price_text = price_el.get_text(strip=True) if price_el else ""
            price_usd, currency = _parse_price(price_text)
            price_raw = price_text if currency == "ARS" else ""

            # --- Features ---
            surface_m2 = 0.0
            rooms = 0

            feature_items = card.select(
                ".card__details li, .listing-details li, "
                ".card-icon-feature, [data-qa='feature']"
            )
            for feat in feature_items:
                text = feat.get_text(strip=True).lower()
                if "m²" in text or "m2" in text:
                    surface_m2 = _parse_surface(text)
                elif "amb" in text or "dorm" in text or "cuart" in text:
                    rooms = _parse_rooms(text)

            # Sometimes surface / rooms appear in icon spans
            if surface_m2 == 0:
                for el in card.select("[class*='sup'], [class*='area'], [class*='m2']"):
                    surface_m2 = _parse_surface(el.get_text(strip=True))
                    if surface_m2:
                        break
            if rooms == 0:
                for el in card.select("[class*='amb'], [class*='room'], [class*='dorm']"):
                    rooms = _parse_rooms(el.get_text(strip=True))
                    if rooms:
                        break

            # --- Location ---
            loc_el = card.select_one(
                ".card__address, .listing-address, "
                "[data-qa='location'], .location"
            )
            barrio = loc_el.get_text(strip=True) if loc_el else neighborhood
            # Argenprop often returns 'Barrio, Ciudad' — keep only the barrio part
            barrio = barrio.split(",")[0].strip()

            # --- URL ---
            link = card.select_one("a[href]")
            url = ""
            if link:
                href = link.get("href", "")
                url = href if href.startswith("http") else BASE_DOMAIN + href

            # --- Title ---
            title_el = card.select_one(
                ".card__title, .listing-title, "
                "[data-qa='title'], h2, h3"
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
            logger.debug("Skipping Argenprop DOM card: %s", exc)

    return raw_listings


def _has_next_page(soup: BeautifulSoup, current_page: int) -> bool:
    """Return True if a next-page control exists."""
    # Argenprop shows an explicit 'Siguiente' link or disables it when last
    next_link = soup.select_one(
        "[data-qa='pagination-next']:not([disabled]), "
        "a[aria-label='Siguiente página'], "
        ".pagination__next:not(.disabled)"
    )
    if next_link:
        return True

    # Fallback: check if any listings were found (covered by caller)
    return False


# ---------------------------------------------------------------------------
# Scraper class
# ---------------------------------------------------------------------------

class ArgenpropScraper(BaseScraper):
    """Scrapes argenprop.com for apartment listings in Buenos Aires."""

    SOURCE = "argenprop"

    def scrape(self) -> list[Listing]:
        listings: list[Listing] = []
        seen_urls: set[str] = set()

        self._start_browser()
        try:
            for neighborhood in self.neighborhoods:
                slug = NEIGHBORHOOD_SLUGS.get(neighborhood)
                if not slug:
                    logger.warning("No URL slug for neighbourhood '%s' — skipping.", neighborhood)
                    continue

                logger.info("[Argenprop] Scraping neighbourhood: %s (slug=%s)", neighborhood, slug)

                for page_num in range(1, self.max_pages + 1):
                    url = _build_url(slug, page_num)
                    logger.debug("  → page %d: %s", page_num, url)

                    page = self._get_page(url)
                    if page is None:
                        logger.warning("  Could not load page %d for %s — stopping.", page_num, neighborhood)
                        break

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
                            logger.debug("  __NEXT_DATA__: found %d items", len(raw))
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
                            amount, cur = _parse_price(item["price_raw"])
                            if cur == "ARS" and amount > 0:
                                price_usd = self.currency.ars_to_usd(amount)

                        if price_usd <= 0 or item["surface_m2"] <= 0:
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

                    if not _has_next_page(soup, page_num):
                        logger.debug("  Last page reached for %s.", neighborhood)
                        break

        finally:
            self._stop_browser()

        logger.info("[Argenprop] Total valid listings collected: %d", len(listings))
        return listings
