"""
database/db.py
==============
Thin wrapper around psycopg2 that handles:
  * Connection management (context-manager safe)
  * Schema initialisation
  * Upsert-on-conflict for listings (dedup by URL)
  * Price-change history tracking
  * Opportunity persistence
  * Neighbourhood reference-price syncing
"""

from __future__ import annotations

import logging
import os
import pathlib
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

from models.listing import Listing

load_dotenv()

logger = logging.getLogger(__name__)

# Path to the SQL schema file sitting next to this module
_SCHEMA_PATH = pathlib.Path(__file__).with_name("schema.sql")


class DatabaseManager:
    """
    Manages all PostgreSQL interactions for the Real Estate Flipper project.

    Usage (preferred — automatic cleanup)::

        with DatabaseManager() as db:
            db.initialize_schema()
            db.upsert_listing(listing)
    """

    # ------------------------------------------------------------------ #
    # Construction / connection                                            #
    # ------------------------------------------------------------------ #

    def __init__(self) -> None:
        self._conn: psycopg2.extensions.connection = self._connect()

    def _connect(self) -> psycopg2.extensions.connection:
        """Open the psycopg2 connection using .env / environment variables."""
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            dbname=os.getenv("DB_NAME", "real_estate_flipper"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
        )
        conn.autocommit = False
        logger.info(
            "Connected to PostgreSQL — %s@%s/%s",
            os.getenv("DB_USER", "postgres"),
            os.getenv("DB_HOST", "localhost"),
            os.getenv("DB_NAME", "real_estate_flipper"),
        )
        return conn

    # ------------------------------------------------------------------ #
    # Context manager                                                      #
    # ------------------------------------------------------------------ #

    def __enter__(self) -> "DatabaseManager":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self._conn.rollback()
        self.close()

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.debug("Database connection closed.")

    # ------------------------------------------------------------------ #
    # Schema                                                               #
    # ------------------------------------------------------------------ #

    def initialize_schema(self) -> None:
        """Create all tables if they do not already exist."""
        sql = _SCHEMA_PATH.read_text(encoding="utf-8")
        with self._conn.cursor() as cur:
            cur.execute(sql)
        self._conn.commit()
        logger.info("Database schema verified / initialised.")

    # ------------------------------------------------------------------ #
    # Listings                                                             #
    # ------------------------------------------------------------------ #

    def upsert_listing(self, listing: Listing) -> Optional[int]:
        """
        Insert a new listing or update ``last_seen`` / ``price_usd`` when the
        URL already exists.  If the price changed, a row is appended to
        ``price_history``.

        Returns the ``listings.id`` on success, or ``None`` on failure.
        """
        sql_upsert = """
            INSERT INTO listings
                (source, title, price_usd, surface_m2, rooms,
                 neighborhood, price_m2, url, first_seen, last_seen)
            VALUES
                (%(source)s, %(title)s, %(price_usd)s, %(surface_m2)s,
                 %(rooms)s, %(neighborhood)s, %(price_m2)s, %(url)s,
                 %(first_seen)s, %(last_seen)s)
            ON CONFLICT (url) DO UPDATE
                SET last_seen = EXCLUDED.last_seen,
                    price_usd = EXCLUDED.price_usd,
                    price_m2  = EXCLUDED.price_m2,
                    title     = EXCLUDED.title
            RETURNING id, (xmax = 0) AS inserted
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    sql_upsert,
                    {
                        "source":       listing.source,
                        "title":        listing.title,
                        "price_usd":    listing.price_usd,
                        "surface_m2":   listing.surface_m2,
                        "rooms":        listing.rooms,
                        "neighborhood": listing.neighborhood,
                        "price_m2":     listing.price_m2,
                        "url":          listing.url,
                        "first_seen":   listing.first_seen,
                        "last_seen":    listing.last_seen,
                    },
                )
                row = cur.fetchone()
                if row is None:
                    self._conn.rollback()
                    logger.error("upsert returned no row for url=%s", listing.url)
                    return None
                listing_id, was_inserted = row

                # Track historical prices on every run so we can spot drops
                self._record_price_history(cur, listing_id, listing)

                self._conn.commit()

                if was_inserted:
                    logger.debug("Inserted new listing id=%s  url=%s", listing_id, listing.url)
                else:
                    logger.debug("Updated listing id=%s  url=%s", listing_id, listing.url)

                return listing_id

        except Exception:
            self._conn.rollback()
            logger.exception("Failed to upsert listing url=%s", listing.url)
            return None

    def _record_price_history(
        self,
        cur: psycopg2.extensions.cursor,
        listing_id: int,
        listing: Listing,
    ) -> None:
        """
        Append to ``price_history`` only when the price differs from the
        most recent recorded price (avoids identical duplicate rows on every
        run when nothing changed).
        """
        cur.execute(
            """
            SELECT price_usd FROM price_history
            WHERE listing_id = %s
            ORDER BY recorded_at DESC
            LIMIT 1
            """,
            (listing_id,),
        )
        prev = cur.fetchone()
        if prev is None or float(prev[0]) != listing.price_usd:
            cur.execute(
                """
                INSERT INTO price_history (listing_id, price_usd, price_m2)
                VALUES (%s, %s, %s)
                """,
                (listing_id, listing.price_usd, listing.price_m2),
            )

    def get_all_listings(self) -> list[Listing]:
        """Fetch every row from ``listings`` and return as ``Listing`` objects."""
        with self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT id, source, title, price_usd, surface_m2, rooms,
                       neighborhood, price_m2, url, first_seen, last_seen
                FROM listings
                ORDER BY last_seen DESC
                """
            )
            rows = cur.fetchall()

        listings: list[Listing] = []
        for row in rows:
            # Use positional args to avoid a Pylance false-positive on DictRow
            # subscript types. Field order matches the dataclass definition:
            # source, title, price_usd, surface_m2, rooms, neighborhood, url
            lst = Listing(
                str(row["source"]),
                str(row["title"]),
                float(row["price_usd"]),
                float(row["surface_m2"]),
                int(row["rooms"] or 0),
                str(row["neighborhood"] or ""),
                str(row["url"]),
            )
            # Restore DB-persisted timestamps and primary key
            lst.first_seen = row["first_seen"]
            lst.last_seen = row["last_seen"]
            lst.id = row["id"]
            listings.append(lst)
        return listings

    # ------------------------------------------------------------------ #
    # Neighbourhood prices                                                 #
    # ------------------------------------------------------------------ #

    def update_neighborhood_prices(self, prices: dict[str, float]) -> None:
        """
        Sync the ``neighborhood_prices`` table from the JSON config dict.

        Args:
            prices: mapping of neighbourhood name → avg USD/m²
        """
        sql = """
            INSERT INTO neighborhood_prices (neighborhood, avg_price_m2, last_updated)
            VALUES (%s, %s, %s)
            ON CONFLICT (neighborhood) DO UPDATE
                SET avg_price_m2  = EXCLUDED.avg_price_m2,
                    last_updated  = EXCLUDED.last_updated
        """
        now = datetime.now()
        try:
            with self._conn.cursor() as cur:
                for neighborhood, avg in prices.items():
                    cur.execute(sql, (neighborhood, avg, now))
            self._conn.commit()
            logger.info("Synced %d neighbourhood price records.", len(prices))
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to update neighbourhood prices.")

    # ------------------------------------------------------------------ #
    # Opportunities                                                        #
    # ------------------------------------------------------------------ #

    def save_opportunity(self, listing_id: int, discount_percentage: float) -> Optional[int]:
        """
        Upsert an opportunity row.  If the listing was already flagged,
        the discount is refreshed and ``detected_at`` stays as the original
        detection time.

        Returns the opportunity ``id``.
        """
        sql = """
            INSERT INTO opportunities (listing_id, discount_percentage, detected_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (listing_id) DO UPDATE
                SET discount_percentage = EXCLUDED.discount_percentage
            RETURNING id
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, (listing_id, round(discount_percentage, 4), datetime.now()))
                result = cur.fetchone()
                if result is None:
                    self._conn.rollback()
                    return None
                opp_id = result[0]
            self._conn.commit()
            return opp_id
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to save opportunity for listing_id=%s", listing_id)
            return None

    def get_opportunities(self) -> list[dict]:
        """
        Return all opportunities joined with their listing details, sorted by
        discount (highest first).
        """
        sql = """
            SELECT
                o.id              AS opp_id,
                o.discount_percentage,
                o.detected_at,
                l.id              AS listing_id,
                l.source,
                l.title,
                l.price_usd,
                l.surface_m2,
                l.rooms,
                l.neighborhood,
                l.price_m2,
                l.url
            FROM opportunities o
            JOIN listings l ON l.id = o.listing_id
            ORDER BY o.discount_percentage DESC
        """
        with self._conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql)
            return [dict(row) for row in cur.fetchall()]
