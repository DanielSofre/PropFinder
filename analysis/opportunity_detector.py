"""
analysis/opportunity_detector.py
==================================
Identifies and ranks real estate flipping opportunities.

A listing is considered an opportunity when ALL of the following hold:
  1. price_m2  <  OPPORTUNITY_THRESHOLD × avg_price_m2  (default: 70%)
  2. rooms     in  [MIN_ROOMS, MAX_ROOMS]                (default: 2–3)
  3. surface   in  [MIN_SURFACE_M2, MAX_SURFACE_M2]     (default: 35–90 m²)
  4. price_usd <   MAX_PRICE_USD                         (default: 150 000 USD)

Discount formula
----------------
discount = 1 − (price_m2 / avg_price_m2)

A discount of 0.30 means the property is 30% below the neighbourhood average.
"""

from __future__ import annotations

import logging
from typing import Optional

from models.listing import Listing
from analysis.price_calculator import discount_vs_market

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds — adjust here or expose via config / CLI flags as needed
# ---------------------------------------------------------------------------

#: Listing must be priced below this fraction of the neighbourhood average
OPPORTUNITY_THRESHOLD: float = 0.70   # i.e. discount > 30%

MIN_ROOMS:       int   = 2
MAX_ROOMS:       int   = 3
MIN_SURFACE_M2:  float = 35.0
MAX_SURFACE_M2:  float = 90.0
MAX_PRICE_USD:   float = 150_000.0


# ---------------------------------------------------------------------------
# Per-listing evaluation
# ---------------------------------------------------------------------------

def is_opportunity(
    listing: Listing,
    avg_price_m2: float,
) -> tuple[bool, Optional[float]]:
    """
    Evaluate a single listing against all opportunity filters.

    Parameters
    ----------
    listing      : the property to evaluate
    avg_price_m2 : neighbourhood reference price (USD/m²)

    Returns
    -------
    (True, discount_fraction)   when all filters pass
    (False, discount_fraction)  when the price qualifies but another filter fails
    (False, None)               when avg_price_m2 is unavailable or price doesn't qualify
    """
    if avg_price_m2 <= 0:
        return False, None

    discount = discount_vs_market(listing.price_m2, avg_price_m2)
    if discount is None:
        return False, None

    # Price filter: must be strictly below the threshold
    if listing.price_m2 >= OPPORTUNITY_THRESHOLD * avg_price_m2:
        return False, discount

    # Room count filter
    if not (MIN_ROOMS <= listing.rooms <= MAX_ROOMS):
        logger.debug(
            "FILTERED (rooms=%d) %s", listing.rooms, listing.url
        )
        return False, discount

    # Surface filter
    if not (MIN_SURFACE_M2 <= listing.surface_m2 <= MAX_SURFACE_M2):
        logger.debug(
            "FILTERED (surface=%.0f m²) %s", listing.surface_m2, listing.url
        )
        return False, discount

    # Absolute price ceiling
    if listing.price_usd >= MAX_PRICE_USD:
        logger.debug(
            "FILTERED (price=%.0f USD) %s", listing.price_usd, listing.url
        )
        return False, discount

    return True, discount


# ---------------------------------------------------------------------------
# Batch detection
# ---------------------------------------------------------------------------

def detect_opportunities(
    listings: list[Listing],
    neighbourhood_averages: dict[str, float],
) -> list[dict]:
    """
    Run ``is_opportunity`` over every listing and return those that qualify,
    sorted by discount in descending order.

    Parameters
    ----------
    listings               : all scraped / loaded Listing objects
    neighbourhood_averages : mapping from neighbourhood name → avg USD/m²

    Returns
    -------
    list of dicts with keys:
        listing              : Listing object
        avg_price_m2         : float
        discount             : fraction (e.g. 0.40 for 40% off)
        discount_percentage  : float (e.g. 40.0)
    """
    opportunities: list[dict] = []

    for listing in listings:
        avg = neighbourhood_averages.get(listing.neighborhood)
        if avg is None:
            logger.debug(
                "No reference price for neighbourhood '%s' — skipping.", listing.neighborhood
            )
            continue

        qualified, discount = is_opportunity(listing, avg)
        if qualified and discount is not None:
            opportunities.append(
                {
                    "listing":             listing,
                    "avg_price_m2":        avg,
                    "discount":            discount,
                    "discount_percentage": round(discount * 100, 2),
                }
            )

    # Sort by discount — highest discount first
    opportunities.sort(key=lambda o: o["discount"], reverse=True)
    return opportunities


# ---------------------------------------------------------------------------
# Pretty printing
# ---------------------------------------------------------------------------

_SEPARATOR = "─" * 56

def print_opportunity(opp: dict) -> None:
    """Print a formatted opportunity block to stdout."""
    lst: Listing = opp["listing"]
    avg: float   = opp["avg_price_m2"]
    disc_pct     = opp["discount_percentage"]

    print(f"\n{'═' * 56}")
    print("  ★  OPPORTUNITY FOUND")
    print(f"{'═' * 56}")
    print(f"  Source          : {lst.source.upper()}")
    print(f"  Neighbourhood   : {lst.neighborhood}")
    print(f"  Title           : {lst.title}")
    print(_SEPARATOR)
    print(f"  Rooms           : {lst.rooms}")
    print(f"  Surface         : {lst.surface_m2:.0f} m²")
    print(f"  Price           : USD {lst.price_usd:,.0f}")
    print(f"  Price / m²      : USD {lst.price_m2:,.0f}")
    print(_SEPARATOR)
    print(f"  Market average  : USD {avg:,.0f} / m²")
    print(f"  Discount        : {disc_pct:.1f}%  ({'⬇' * min(int(disc_pct // 10), 5)})")
    print(_SEPARATOR)
    print(f"  Listing URL     :\n  {lst.url}")
    print(f"{'═' * 56}\n")


def print_summary(opportunities: list[dict], total_listings: int) -> None:
    """Print a concise summary table."""
    print(f"\n{'═' * 56}")
    print(f"  SCAN COMPLETE — {total_listings} listings analysed")
    print(f"  {len(opportunities)} opportunities found")
    print(f"{'═' * 56}")

    if not opportunities:
        print("  No opportunities matched the current filters.")
        print(f"  Filters: rooms {MIN_ROOMS}–{MAX_ROOMS}, "
              f"surface {MIN_SURFACE_M2:.0f}–{MAX_SURFACE_M2:.0f} m², "
              f"price < USD {MAX_PRICE_USD:,.0f}, "
              f"discount > {int((1 - OPPORTUNITY_THRESHOLD) * 100)}%")
        return

    print(
        f"\n  {'#':<4} {'Neighbourhood':<20} {'Rooms':<6} "
        f"{'m²':<6} {'Price USD':>10} {'Disc%':>6}"
    )
    print(f"  {'-'*4} {'-'*20} {'-'*6} {'-'*6} {'-'*10} {'-'*6}")
    for i, opp in enumerate(opportunities, 1):
        lst = opp["listing"]
        print(
            f"  {i:<4} {lst.neighborhood:<20} {lst.rooms:<6} "
            f"{lst.surface_m2:<6.0f} {lst.price_usd:>10,.0f} "
            f"{opp['discount_percentage']:>5.1f}%"
        )
    print()
