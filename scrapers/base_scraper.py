"""
scrapers/base_scraper.py
========================
Abstract base class shared by all site-specific scrapers.

Responsibilities
----------------
* Launch / close a Playwright browser instance
* Provide a helper to fetch a page with retry logic and realistic delays
* Provide USD conversion (fetches the Argentine "blue dollar" rate once per run)
* Define the interface every scraper must implement
"""

from __future__ import annotations

import logging
import random
import time
from abc import ABC, abstractmethod
from typing import Optional

import requests
from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    sync_playwright,
)

from models.listing import Listing

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

_BLUE_DOLLAR_API = "https://dolarapi.com/v1/dolares/blue"
_OFFICIAL_DOLLAR_API = "https://dolarapi.com/v1/dolares/oficial"


# ---------------------------------------------------------------------------
# Currency helper
# ---------------------------------------------------------------------------

class CurrencyConverter:
    """
    Fetches the Argentine blue-dollar and official exchange rates once,
    then reuses the cached values for all conversions within a single run.
    """

    def __init__(self) -> None:
        self._blue_rate: Optional[float] = None
        self._official_rate: Optional[float] = None

    def _fetch_rate(self, url: str, fallback: float) -> float:
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            # Use the "venta" (sell) rate — the one buyers effectively pay
            return float(data.get("venta", fallback))
        except Exception as exc:
            logger.warning("Could not fetch exchange rate from %s: %s — using fallback %s", url, exc, fallback)
            return fallback

    @property
    def blue_rate(self) -> float:
        if self._blue_rate is None:
            self._blue_rate = self._fetch_rate(_BLUE_DOLLAR_API, fallback=1_100.0)
            logger.info("Blue dollar rate (ARS→USD sell): %.2f", self._blue_rate)
        return self._blue_rate

    @property
    def official_rate(self) -> float:
        if self._official_rate is None:
            self._official_rate = self._fetch_rate(_OFFICIAL_DOLLAR_API, fallback=900.0)
            logger.info("Official dollar rate (ARS→USD sell): %.2f", self._official_rate)
        return self._official_rate

    def ars_to_usd(self, ars_amount: float, use_blue: bool = True) -> float:
        """Convert Argentine Pesos to US Dollars."""
        rate = self.blue_rate if use_blue else self.official_rate
        return round(ars_amount / rate, 2)


# ---------------------------------------------------------------------------
# Base scraper
# ---------------------------------------------------------------------------

class BaseScraper(ABC):
    """
    Abstract base for Zonaprop / Argenprop scrapers.

    Subclasses must implement:
      * ``scrape()``  — orchestrates per-neighbourhood pagination

    Parameters
    ----------
    neighborhoods : list of neighbourhood names (as they appear in the config)
    max_pages     : how many result pages to scrape per neighbourhood
    headless      : run browser without UI (True in production)
    delay_range   : (min_s, max_s) random sleep between page loads
    """

    def __init__(
        self,
        neighborhoods: list[str],
        max_pages: int = 5,
        headless: bool = True,
        delay_range: tuple[float, float] = (2.5, 5.5),
    ) -> None:
        self.neighborhoods = neighborhoods
        self.max_pages = max_pages
        self.headless = headless
        self.delay_range = delay_range
        self.currency = CurrencyConverter()
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None

    # ------------------------------------------------------------------ #
    # Browser lifecycle                                                    #
    # ------------------------------------------------------------------ #

    # JavaScript snippet injected into every page before any scripts run.
    # Overrides the standard Playwright/CDP automation signals that Cloudflare
    # and similar bot-detection services check.
    _STEALTH_INIT_SCRIPT = """
        // Remove the 'webdriver' property that automation frameworks set
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

        // Spoof plugins array (empty in headless Chrome)
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });

        // Spoof language
        Object.defineProperty(navigator, 'languages', {
            get: () => ['es-AR', 'es', 'en'],
        });

        // Chrome object is absent in some headless builds
        window.chrome = window.chrome || { runtime: {} };

        // Permissions API — headless returns 'denied' by default which is a tell
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : originalQuery(parameters)
        );
    """

    def _start_browser(self) -> None:
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        self._context = self._browser.new_context(
            user_agent=random.choice(_DEFAULT_USER_AGENTS),
            locale="es-AR",
            timezone_id="America/Argentina/Buenos_Aires",
            viewport={"width": 1366, "height": 768},
            extra_http_headers={
                "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        # Inject stealth script before any page scripts run
        self._context.add_init_script(self._STEALTH_INIT_SCRIPT)
        # Block unnecessary resource types to speed up scraping
        # NOTE: keep 'script' unblocked — JS is needed to render listings
        self._context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ("image", "media", "font")
            else route.continue_(),
        )
        logger.debug("%s browser started (headless=%s).", self.__class__.__name__, self.headless)

    def _stop_browser(self) -> None:
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
        logger.debug("%s browser stopped.", self.__class__.__name__)

    def _rotate_context(self) -> None:
        """
        Close the current browser context and open a fresh one.

        Call this between scraping sessions (e.g. between neighbourhoods) so
        that Cloudflare and similar bot-detection systems see a brand-new
        client with a clean cookie jar and session storage.
        """
        if self._context:
            self._context.close()
        if self._browser is None:
            raise RuntimeError("_rotate_context() called before _start_browser()")
        self._context = self._browser.new_context(
            user_agent=random.choice(_DEFAULT_USER_AGENTS),
            locale="es-AR",
            timezone_id="America/Argentina/Buenos_Aires",
            viewport={"width": 1366, "height": 768},
            extra_http_headers={
                "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        self._context.add_init_script(self._STEALTH_INIT_SCRIPT)
        self._context.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in ("image", "media", "font")
            else route.continue_(),
        )
        logger.debug("Browser context rotated (fresh session).")

    # ------------------------------------------------------------------ #
    # Page fetching                                                        #
    # ------------------------------------------------------------------ #

    def _get_page(self, url: str, retries: int = 3) -> Optional[Page]:
        """
        Open ``url`` in a fresh browser page with retry logic.

        Returns the ``Page`` object on success, or ``None`` after all retries
        are exhausted.
        """
        context = self._context
        if context is None:
            raise RuntimeError("_get_page() called before _start_browser()")
        for attempt in range(1, retries + 1):
            page: Page = context.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                self._random_delay()
                return page
            except Exception as exc:
                logger.warning(
                    "Attempt %d/%d — failed to load %s: %s", attempt, retries, url, exc
                )
                page.close()
                if attempt < retries:
                    time.sleep(random.uniform(3, 7))
        return None

    def _random_delay(self) -> None:
        delay = random.uniform(*self.delay_range)
        time.sleep(delay)

    # ------------------------------------------------------------------ #
    # Interface                                                            #
    # ------------------------------------------------------------------ #

    @abstractmethod
    def scrape(self) -> list[Listing]:
        """
        Entry point called by ``main.py``.
        Must return a list of validated ``Listing`` objects.
        """

    # ------------------------------------------------------------------ #
    # Context manager support                                              #
    # ------------------------------------------------------------------ #

    def __enter__(self) -> "BaseScraper":
        self._start_browser()
        return self

    def __exit__(self, *args) -> None:
        self._stop_browser()
