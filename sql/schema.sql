-- =============================================================================
-- schema.sql
-- DuckDB schema for the Automated Reporting Pipeline
--
-- Tables:
--   customers     — dimension table: customer master data
--   products      — dimension table: product catalogue
--   orders        — fact table: individual order lines
--   deliveries    — fact table: delivery events
--   fact          — denormalised analytical fact table (pre-joined)
--
-- All DROP IF EXISTS statements ensure the schema is idempotent:
-- re-running the pipeline recreates clean tables each time.
-- =============================================================================

-- ── Dimensions ────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id     VARCHAR PRIMARY KEY,
    customer_name   VARCHAR NOT NULL,
    segment         VARCHAR,
    city            VARCHAR,
    country         VARCHAR,
    signup_date     DATE
);

DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id      VARCHAR PRIMARY KEY,
    product_name    VARCHAR NOT NULL,
    category        VARCHAR,
    unit_cost       DOUBLE
);

-- ── Facts ─────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id        VARCHAR PRIMARY KEY,
    customer_id     VARCHAR,
    product_id      VARCHAR,
    order_date      DATE,
    quantity        INTEGER,
    unit_price      DOUBLE,
    order_status    VARCHAR,
    revenue         DOUBLE
);

DROP TABLE IF EXISTS deliveries;
CREATE TABLE deliveries (
    delivery_id             VARCHAR PRIMARY KEY,
    order_id                VARCHAR,
    carrier                 VARCHAR,
    shipped_date            DATE,
    delivered_date          DATE,
    delivery_status         VARCHAR,
    delivery_delay_days     INTEGER
);

-- ── Analytical fact table (denormalised) ──────────────────────────────────────

DROP TABLE IF EXISTS fact;
CREATE TABLE fact (
    -- order dimensions
    order_id            VARCHAR,
    order_date          DATE,
    order_status        VARCHAR,
    revenue             DOUBLE,
    quantity            INTEGER,
    unit_price          DOUBLE,
    year_month          VARCHAR,
    -- customer dimensions
    customer_id         VARCHAR,
    customer_name       VARCHAR,
    segment             VARCHAR,
    city                VARCHAR,
    country             VARCHAR,
    -- product dimensions
    product_id          VARCHAR,
    product_name        VARCHAR,
    category            VARCHAR,
    -- delivery dimensions
    delivery_id         VARCHAR,
    delivery_status     VARCHAR,
    delivery_delay_days INTEGER,
    shipped_date        DATE,
    delivered_date      DATE,
    carrier             VARCHAR
);
