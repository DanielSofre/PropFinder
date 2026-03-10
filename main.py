"""
main.py
=======
Entry point for the Real Estate Flipper.

Execution order
---------------
1. Load .env variables
2. Read neighbourhood reference prices from config/neighborhood_prices.json
3. Initialise the PostgreSQL schema (idempotent — safe to run repeatedly)
4. Sync reference prices into the ``neighbourhood_prices`` table
5. Run the Zonaprop scraper for every configured neighbourhood
6. Run the Argenprop scraper for every configured neighbourhood
7. Upsert all collected listings into ``listings`` (tracks price history)
8. Detect opportunities and persist them into ``opportunities``
9. Print a formatted report to stdout

Usage
-----
    python main.py [--max-pages N] [--no-headless] [--source zonaprop|argenprop|both]

Flags
-----
  --max-pages N       Maximum result pages to scrape per neighbourhood (default: 5)
  --no-headless       Show the browser window (useful for debugging)
  --source            Which scraper(s) to run: zonaprop | argenprop | both  (default: both)
  --dry-run           Scrape & analyse without writing to the database
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime

from dotenv import load_dotenv

from analysis.opportunity_detector import (
    detect_opportunities,
    print_opportunity,
    print_summary,
)
from analysis.price_calculator import (
    build_analysis_dataframe,
    compute_market_averages,
    load_neighborhood_averages,
    summarise_by_neighborhood,
)
from database.db import DatabaseManager
from models.listing import Listing
from scrapers.argenprop_scraper import ArgenpropScraper
from scrapers.zonaprop_scraper import ZonapropScraper

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("main")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Buenos Aires real-estate flip-opportunity detector."
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        metavar="N",
        help="Pages to scrape per neighbourhood per source (default: 5).",
    )
    parser.add_argument(
        "--no-headless",
        action="store_true",
        help="Show the browser window (useful for debugging selectors).",
    )
    parser.add_argument(
        "--source",
        choices=["zonaprop", "argenprop", "both"],
        default="both",
        help="Which scraper(s) to run (default: both).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape and analyse without persisting to the database.",
    )
    parser.add_argument(
        "--redetect-only",
        action="store_true",
        help="Re-run opportunity detection on existing DB listings (no scraping).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _redetect_only(neighbourhood_averages: dict) -> None:
    """Re-detect opportunities from existing DB listings without scraping."""
    logger.info("--redetect-only: loading listings from database…")
    try:
        with DatabaseManager() as db:
            db.initialize_schema()
            all_listings = db.get_all_listings()
            logger.info("Loaded %d listings from DB.", len(all_listings))

            # Compute market averages from existing listings, fallback to JSON
            neighbourhood_averages = compute_market_averages(all_listings, neighbourhood_averages)
            db.update_neighborhood_prices(neighbourhood_averages)

            opportunities = detect_opportunities(all_listings, neighbourhood_averages)

            db.clear_opportunities()
            saved_opps = 0
            for opp in opportunities:
                lst = opp["listing"]
                if lst.id:
                    db.save_opportunity(lst.id, opp["discount_percentage"])
                    saved_opps += 1

            logger.info("Saved %d opportunities to PostgreSQL.", saved_opps)

    except Exception:
        logger.exception("Re-detection failed.")
        sys.exit(1)

    print_summary(opportunities, total_listings=len(all_listings))
    for opp in opportunities:
        print_opportunity(opp)


def main() -> None:
    args = _parse_args()

    start_time = datetime.now()
    logger.info("═" * 60)
    logger.info("  Real Estate Flipper — Buenos Aires")
    logger.info("  Started at %s", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("═" * 60)

    # ------------------------------------------------------------------ #
    # 1. Load neighbourhood reference prices                               #
    # ------------------------------------------------------------------ #
    try:
        neighbourhood_averages = load_neighborhood_averages()
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)

    if args.redetect_only:
        _redetect_only(neighbourhood_averages)
        return

    neighbourhoods = list(neighbourhood_averages.keys())
    logger.info(
        "Analysing %d neighbourhood(s): %s",
        len(neighbourhoods),
        ", ".join(neighbourhoods),
    )

    headless = not args.no_headless

    # ------------------------------------------------------------------ #
    # 2. Run scrapers                                                      #
    # ------------------------------------------------------------------ #
    all_listings: list[Listing] = []

    scraper_classes = []
    if args.source in ("zonaprop", "both"):
        scraper_classes.append(ZonapropScraper)
    if args.source in ("argenprop", "both"):
        scraper_classes.append(ArgenpropScraper)

    for ScraperClass in scraper_classes:
        scraper = ScraperClass(
            neighborhoods=neighbourhoods,
            max_pages=args.max_pages,
            headless=headless,
        )
        logger.info("Running %s …", ScraperClass.__name__)
        try:
            batch = scraper.scrape()
            logger.info("  ✓ %s returned %d valid listings.", ScraperClass.__name__, len(batch))
            all_listings.extend(batch)
        except Exception:
            logger.exception("  ✗ %s failed — continuing.", ScraperClass.__name__)

    logger.info("Total listings collected: %d", len(all_listings))

    if not all_listings:
        logger.warning("No listings were scraped. Check network access and scraper selectors.")
        sys.exit(0)

    # ------------------------------------------------------------------ #
    # 2b. Compute market averages from scraped data (fallback to JSON)    #
    # ------------------------------------------------------------------ #
    neighbourhood_averages = compute_market_averages(all_listings, neighbourhood_averages)

    # ------------------------------------------------------------------ #
    # 3. Persist listings (unless --dry-run)                              #
    # ------------------------------------------------------------------ #
    if not args.dry_run:
        try:
            with DatabaseManager() as db:
                db.initialize_schema()
                db.update_neighborhood_prices(neighbourhood_averages)

                stored = 0
                for listing in all_listings:
                    if listing.is_valid():
                        listing_id = db.upsert_listing(listing)
                        if listing_id:
                            listing.id = listing_id
                            stored += 1

                logger.info("Persisted %d listings to PostgreSQL.", stored)

                # -------------------------------------------------------- #
                # 4. Detect & persist opportunities                         #
                # -------------------------------------------------------- #
                opportunities = detect_opportunities(all_listings, neighbourhood_averages)

                saved_opps = 0
                for opp in opportunities:
                    lst = opp["listing"]
                    if lst.id:
                        db.save_opportunity(lst.id, opp["discount_percentage"])
                        saved_opps += 1

                logger.info("Saved %d opportunities to PostgreSQL.", saved_opps)

        except Exception:
            logger.exception(
                "Database error — falling back to in-memory analysis only."
            )
            opportunities = detect_opportunities(all_listings, neighbourhood_averages)
    else:
        logger.info("--dry-run active: skipping database writes.")
        opportunities = detect_opportunities(all_listings, neighbourhood_averages)

    # ------------------------------------------------------------------ #
    # 5. Build analysis dataframe & neighbourhood summary                  #
    # ------------------------------------------------------------------ #
    df = build_analysis_dataframe(all_listings, neighbourhood_averages)
    if not df.empty:
        summary = summarise_by_neighborhood(df)
        logger.info("\nNeighbourhood Summary:\n%s", summary.to_string())

    # ------------------------------------------------------------------ #
    # 6. Print opportunity report                                          #
    # ------------------------------------------------------------------ #
    print_summary(opportunities, total_listings=len(all_listings))

    for opp in opportunities:
        print_opportunity(opp)

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("Finished in %.1f seconds.", elapsed)


if __name__ == "__main__":
    main()
