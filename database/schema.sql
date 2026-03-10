CREATE TABLE IF NOT EXISTS listings (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    title VARCHAR(500) NOT NULL,
    price_usd NUMERIC(10, 2) NOT NULL,
    surface_m2 NUMERIC(10, 2) NOT NULL,
    rooms INTEGER NOT NULL,
    neighborhood VARCHAR(100) NOT NULL,
    condition VARCHAR(50) NOT NULL DEFAULT '',
    price_m2 NUMERIC(10, 2) NOT NULL,
    url TEXT UNIQUE NOT NULL,
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL
);
