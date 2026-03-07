# Real Estate Flipper — Buenos Aires

A Python tool that automatically scrapes real estate listing portals
(**Zonaprop** and **Argenprop**), stores listings in **PostgreSQL**, and
detects properties priced significantly below their neighbourhood market
average — potential flipping opportunities.

---

## Table of Contents

1. [Requirements](#requirements)
2. [Installation](#installation)
3. [Database setup](#database-setup)
4. [Configuration](#configuration)
5. [Running the scraper](#running-the-scraper)
6. [Project structure](#project-structure)
7. [Opportunity detection logic](#opportunity-detection-logic)
8. [Output example](#output-example)
9. [Extending the project](#extending-the-project)

---

## Requirements

| Tool       | Version  |
|------------|----------|
| Python     | 3.11+    |
| PostgreSQL | 14+      |
| pip        | 23+      |

---

## Installation

```bash
# 1. Clone / enter the project directory
cd real_estate_flipper

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate.bat     # Windows

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Install Playwright browser binaries
playwright install chromium
```

---

## Database Setup

### 1. Create the database

```bash
psql -U postgres -c "CREATE DATABASE real_estate_flipper;"
```

### 2. Configure the connection

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

```ini
# .env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=real_estate_flipper
DB_USER=postgres
DB_PASSWORD=your_password_here
```

### 3. Initialise the schema

The schema is applied automatically on the first run of `main.py`.
You can also apply it manually:

```bash
psql -U postgres -d real_estate_flipper -f database/schema.sql
```

---

## Configuration

### Neighbourhood reference prices

Edit `config/neighborhood_prices.json` to set or update the average
USD/m² for each neighbourhood you want to track:

```json
{
  "Caballito":    2200,
  "Almagro":      2000,
  "Villa Crespo": 2100,
  "Flores":       1700,
  "Palermo":      3200
}
```

> **Tip:** Use recent data from portals like Properati or Reporte
> Inmobiliario to keep these figures accurate.

Neighbourhoods that are **not** listed in this file are scraped but
excluded from the opportunity analysis.

---

## Running the Scraper

```bash
# Run both scrapers (default — 5 pages per neighbourhood)
python main.py

# Limit to 2 pages per neighbourhood
python main.py --max-pages 2

# Run only Zonaprop
python main.py --source zonaprop

# Run only Argenprop
python main.py --source argenprop

# Show the browser window (useful for debugging)
python main.py --no-headless

# Scrape and analyse without writing to the database
python main.py --dry-run
```

---

## Project Structure

```
real_estate_flipper/
│
├── scrapers/
│   ├── base_scraper.py          # Abstract base + Playwright lifecycle + currency conversion
│   ├── zonaprop_scraper.py      # Zonaprop.com.ar scraper
│   └── argenprop_scraper.py     # Argenprop.com scraper
│
├── database/
│   ├── db.py                    # DatabaseManager (psycopg2 wrapper)
│   └── schema.sql               # Table definitions
│
├── analysis/
│   ├── price_calculator.py      # USD/m² calculations + pandas summaries
│   └── opportunity_detector.py  # Filter logic + pretty printing
│
├── config/
│   └── neighborhood_prices.json # Reference prices (manually maintained)
│
├── models/
│   └── listing.py               # Listing dataclass
│
├── main.py                      # Entry point
├── requirements.txt
├── .env.example
└── README.md
```

---

## Opportunity Detection Logic

A listing is flagged as an **opportunity** when **all** of the following
conditions are met simultaneously:

| Filter                | Rule                                  |
|-----------------------|---------------------------------------|
| Price per m²          | `< 70%` of neighbourhood average      |
| Rooms                 | 2 or 3                                |
| Surface               | 35 m² – 90 m²                        |
| Asking price          | `< USD 150 000`                       |

**Discount formula:**

```
discount = 1 − (price_m2 / avg_price_m2)
```

A discount of `0.40` means the property is **40% below** the market
average for that neighbourhood.

---

### Tuning the filters

All thresholds live in `analysis/opportunity_detector.py` as
module-level constants:

```python
OPPORTUNITY_THRESHOLD = 0.70   # must be below 70% of avg → discount > 30%
MIN_ROOMS             = 2
MAX_ROOMS             = 3
MIN_SURFACE_M2        = 35.0
MAX_SURFACE_M2        = 90.0
MAX_PRICE_USD         = 150_000.0
```

---

## Output Example

```
════════════════════════════════════════════════════════
  ★  OPPORTUNITY FOUND
════════════════════════════════════════════════════════
  Source          : ZONAPROP
  Neighbourhood   : Caballito
  Title           : Departamento 2 ambientes en venta
────────────────────────────────────────────────────────
  Rooms           : 2
  Surface         : 42 m²
  Price           : USD 55,000
  Price / m²      : USD 1,309
────────────────────────────────────────────────────────
  Market average  : USD 2,200 / m²
  Discount        : 40.5%  (⬇⬇⬇⬇)
────────────────────────────────────────────────────────
  Listing URL     :
  https://www.zonaprop.com.ar/propiedades/departamento-2-amb-caballito-123456.html
════════════════════════════════════════════════════════
```

---

## Database Tables

### `listings`
| Column        | Type           | Description                          |
|---------------|----------------|--------------------------------------|
| id            | SERIAL PK      | Auto-incremented primary key         |
| source        | VARCHAR(50)    | `zonaprop` or `argenprop`            |
| title         | TEXT           | Raw listing title                    |
| price_usd     | NUMERIC(12,2)  | Asking price in USD                  |
| surface_m2    | NUMERIC(8,2)   | Covered area in m²                   |
| rooms         | INTEGER        | Number of rooms (ambientes)          |
| neighborhood  | VARCHAR(100)   | Neighbourhood name                   |
| price_m2      | NUMERIC(10,2)  | Computed USD/m²                      |
| url           | TEXT UNIQUE    | Listing URL (deduplication key)      |
| first_seen    | TIMESTAMP      | When the listing was first scraped   |
| last_seen     | TIMESTAMP      | Most recent scrape timestamp         |

### `neighborhood_prices`
| Column        | Type           | Description                          |
|---------------|----------------|--------------------------------------|
| id            | SERIAL PK      |                                      |
| neighborhood  | VARCHAR(100)   | Unique neighbourhood name            |
| avg_price_m2  | NUMERIC(10,2)  | Reference USD/m²                     |
| last_updated  | TIMESTAMP      | When the value was last synced       |

### `price_history`
| Column        | Type           | Description                          |
|---------------|----------------|--------------------------------------|
| id            | SERIAL PK      |                                      |
| listing_id    | INTEGER FK     | References `listings.id`             |
| price_usd     | NUMERIC(12,2)  | Price at this point in time          |
| price_m2      | NUMERIC(10,2)  | Computed USD/m² at this point        |
| recorded_at   | TIMESTAMP      | When this price was observed         |

### `opportunities`
| Column              | Type          | Description                       |
|---------------------|---------------|-----------------------------------|
| id                  | SERIAL PK     |                                   |
| listing_id          | INTEGER FK    | References `listings.id`          |
| discount_percentage | NUMERIC(5,2)  | e.g. `40.50` for a 40.5% discount |
| detected_at         | TIMESTAMP     | When the opportunity was flagged  |

---

## Extending the Project

* **Add a new portal:** Create a new file in `scrapers/` that subclasses
  `BaseScraper` and implements `scrape()`, then register it in `main.py`.
* **Add more neighbourhoods:** Add entries to `config/neighborhood_prices.json`
  and map the slug in the scraper's `NEIGHBORHOOD_SLUGS` dict.
* **Schedule recurring runs:** Use `cron` or a task scheduler to run
  `python main.py` daily and track price changes over time via `price_history`.
* **Export results:** Use `analysis/price_calculator.py`'s `build_analysis_dataframe()`
  to export to CSV: `df.to_csv("report.csv", index=False)`.
* **Alerting:** Add an email or Telegram notification step at the end of
  `main.py` when `opportunities` is non-empty.
