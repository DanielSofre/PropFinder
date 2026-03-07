"""
analysis/price_calculator.py
=============================
Utilities for loading neighbourhood reference prices and computing per-m²
statistics across a collection of listings.
"""

from __future__ import annotations

import json
import logging
import pathlib
from typing import Optional, cast

import pandas as pd

from models.listing import Listing

logger = logging.getLogger(__name__)

_CONFIG_PATH = (
    pathlib.Path(__file__).resolve().parent.parent / "config" / "neighborhood_prices.json"
)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_neighborhood_averages(path: pathlib.Path = _CONFIG_PATH) -> dict[str, float]:
    """
    Read ``neighborhood_prices.json`` and return a mapping of
    ``{neighbourhood_name: avg_price_per_m2_usd}``.

    Raises
    ------
    FileNotFoundError
        When the config file cannot be located.
    ValueError
        When the JSON is malformed or contains non-numeric price values.
    """
    if not path.exists():
        raise FileNotFoundError(
            f"Neighbourhood price config not found at: {path}\n"
            "Create or copy config/neighborhood_prices.json before running."
        )

    raw = json.loads(path.read_text(encoding="utf-8"))

    prices: dict[str, float] = {}
    for neighbourhood, value in raw.items():
        try:
            prices[neighbourhood] = float(value)
        except (TypeError, ValueError):
            raise ValueError(
                f"Invalid price value for '{neighbourhood}': {value!r}. "
                "Expected a number (USD per m²)."
            )

    logger.info("Loaded reference prices for %d neighbourhoods.", len(prices))
    return prices


# ---------------------------------------------------------------------------
# Per-listing calculations
# ---------------------------------------------------------------------------

def calculate_price_per_m2(price_usd: float, surface_m2: float) -> float:
    """Return USD/m² rounded to 2 decimal places, or 0 on bad input."""
    if surface_m2 <= 0 or price_usd <= 0:
        return 0.0
    return round(price_usd / surface_m2, 2)


def discount_vs_market(price_m2: float, avg_price_m2: float) -> Optional[float]:
    """
    Compute the relative discount of a listing vs the neighbourhood average.

    Formula
    -------
    discount = 1 − (price_m2 / avg_price_m2)

    Returns
    -------
    float
        Discount as a fraction in [−∞, 1). Positive values mean the listing
        is cheaper than the market. Returns ``None`` if ``avg_price_m2`` is 0.
    """
    if avg_price_m2 <= 0:
        return None
    return round(1 - (price_m2 / avg_price_m2), 6)


# ---------------------------------------------------------------------------
# Batch analysis with pandas
# ---------------------------------------------------------------------------

def build_analysis_dataframe(
    listings: list[Listing],
    neighbourhood_averages: dict[str, float],
) -> pd.DataFrame:
    """
    Convert a list of ``Listing`` objects into a ``pandas.DataFrame`` enriched
    with market-comparison columns.

    Columns added
    -------------
    avg_price_m2       : neighbourhood reference price from the config
    discount           : fraction below market (positive = cheaper)
    discount_pct       : ``discount`` expressed as a percentage string
    """
    if not listings:
        return pd.DataFrame()

    rows = [lst.to_dict() for lst in listings]
    df = pd.DataFrame(rows)

    # Ensure numeric types
    df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")
    df["surface_m2"] = pd.to_numeric(df["surface_m2"], errors="coerce")
    rooms_numeric = cast("pd.Series[float]", pd.to_numeric(df["rooms"], errors="coerce"))
    df["rooms"] = rooms_numeric.fillna(0).astype(int)
    df["price_m2"] = pd.to_numeric(df["price_m2"], errors="coerce")

    # Map neighbourhood averages
    df["avg_price_m2"] = df["neighborhood"].map(neighbourhood_averages)

    # Compute discount
    df["discount"] = df.apply(
        lambda r: discount_vs_market(r["price_m2"], r["avg_price_m2"])
        if pd.notna(r["avg_price_m2"])
        else None,
        axis=1,
    )
    df["discount_pct"] = df["discount"].apply(
        lambda d: f"{d * 100:.1f}%" if d is not None else "N/A"
    )

    return df


def summarise_by_neighborhood(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a summary DataFrame with per-neighbourhood listing statistics.

    Includes: count, mean/median price, mean/median price_m2.
    """
    if df.empty:
        return pd.DataFrame()

    return (
        df.groupby("neighborhood")
        .agg(
            listings=("id", "count"),
            avg_price_usd=("price_usd", "mean"),
            median_price_usd=("price_usd", "median"),
            avg_price_m2_scraped=("price_m2", "mean"),
            median_price_m2_scraped=("price_m2", "median"),
            ref_price_m2=("avg_price_m2", "first"),
        )
        .round(2)
        .sort_values("listings", ascending=False)
    )
