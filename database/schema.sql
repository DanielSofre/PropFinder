-- =============================================================================
-- Real Estate Flipper — PostgreSQL Schema
-- Run this file once to initialise the database:
--   psql -U <user> -d <dbname> -f schema.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- listings
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS listings (
    id          SERIAL PRIMARY KEY,
    source      VARCHAR(50)     NOT NULL,
    title       TEXT            NOT NULL,
    price_usd   NUMERIC(15, 2)  NOT NULL,
    surface_m2  NUMERIC(8,  2)  NOT NULL,
    rooms       INTEGER,
    neighborhood VARCHAR(100),
    price_m2    NUMERIC(12, 2),
    url         TEXT            UNIQUE NOT NULL,
    first_seen  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_listings_neighborhood ON listings (neighborhood);
CREATE INDEX IF NOT EXISTS idx_listings_price_m2     ON listings (price_m2);
CREATE INDEX IF NOT EXISTS idx_listings_source       ON listings (source);

-- ---------------------------------------------------------------------------
-- neighborhood_prices  (reference table — populated from neighborhood_prices.json)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS neighborhood_prices (
    id            SERIAL PRIMARY KEY,
    neighborhood  VARCHAR(100)    UNIQUE NOT NULL,
    avg_price_m2  NUMERIC(10, 2)  NOT NULL,
    last_updated  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- price_history  (tracks price changes for a given listing over time)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS price_history (
    id          SERIAL PRIMARY KEY,
    listing_id  INTEGER         NOT NULL REFERENCES listings (id) ON DELETE CASCADE,
    price_usd   NUMERIC(15, 2)  NOT NULL,
    price_m2    NUMERIC(12, 2)  NOT NULL,
    recorded_at TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_price_history_listing ON price_history (listing_id);

-- ---------------------------------------------------------------------------
-- opportunities
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS opportunities (
    id                  SERIAL PRIMARY KEY,
    listing_id          INTEGER        NOT NULL REFERENCES listings (id) ON DELETE CASCADE,
    discount_percentage NUMERIC(5, 2)  NOT NULL,
    detected_at         TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_opportunity_listing UNIQUE (listing_id)
);

CREATE INDEX IF NOT EXISTS idx_opportunities_discount ON opportunities (discount_percentage DESC);
