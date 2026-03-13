-- =============================================================================
-- kpi_queries.sql
-- Reference SQL KPI queries for the Automated Reporting Pipeline
--
-- These queries document the authoritative KPI definitions and can be run
-- independently in any DuckDB client (e.g., DBeaver, CLI) against pipeline.duckdb.
--
-- Engine: DuckDB (ANSI SQL + DuckDB extensions)
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- 1. COMMERCIAL METRICS
-- ─────────────────────────────────────────────────────────────────────────────

-- Total number of orders (all statuses)
SELECT COUNT(DISTINCT order_id) AS total_orders
FROM orders;

-- Total revenue from completed orders only
SELECT ROUND(SUM(revenue), 2) AS total_revenue
FROM fact
WHERE order_status = 'completed';

-- Average order value (completed orders)
SELECT ROUND(AVG(revenue), 2) AS average_order_value
FROM fact
WHERE order_status = 'completed';

-- Total distinct customers
SELECT COUNT(DISTINCT customer_id) AS total_customers
FROM customers;

-- Cancellation rate
SELECT
    ROUND(
        SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(COUNT(*), 0),
        4
    ) AS cancellation_rate
FROM fact;

-- Return rate
SELECT
    ROUND(
        SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(COUNT(*), 0),
        4
    ) AS return_rate
FROM fact;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. DELIVERY METRICS
-- ─────────────────────────────────────────────────────────────────────────────

-- On-time delivery rate
SELECT
    ROUND(
        SUM(CASE WHEN delivery_status = 'on_time' THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN delivery_status IN ('on_time','late') THEN 1 ELSE 0 END), 0),
        4
    ) AS on_time_delivery_rate
FROM deliveries;

-- Late delivery rate
SELECT
    ROUND(
        SUM(CASE WHEN delivery_status = 'late' THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(SUM(CASE WHEN delivery_status IN ('on_time','late') THEN 1 ELSE 0 END), 0),
        4
    ) AS late_delivery_rate
FROM deliveries;

-- Average delivery delay (days from ship to delivery)
SELECT ROUND(AVG(delivery_delay_days), 2) AS average_delivery_delay_days
FROM deliveries
WHERE delivery_delay_days IS NOT NULL;

-- Delivery performance by carrier
SELECT
    carrier,
    COUNT(*)                                                            AS total_deliveries,
    SUM(CASE WHEN delivery_status = 'on_time' THEN 1 ELSE 0 END)       AS on_time_count,
    ROUND(
        SUM(CASE WHEN delivery_status = 'on_time' THEN 1 ELSE 0 END)::DOUBLE
        / NULLIF(COUNT(*), 0),
        4
    )                                                                   AS on_time_rate,
    ROUND(AVG(delivery_delay_days), 1)                                  AS avg_delay_days
FROM deliveries
WHERE delivery_status IN ('on_time', 'late')
GROUP BY carrier
ORDER BY on_time_rate DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. PRODUCT ANALYSIS
-- ─────────────────────────────────────────────────────────────────────────────

-- Top 5 products by revenue
SELECT
    product_id,
    product_name,
    ROUND(SUM(revenue), 2)  AS total_revenue,
    SUM(quantity)           AS total_units_sold,
    COUNT(order_id)         AS order_count
FROM fact
WHERE order_status = 'completed'
GROUP BY product_id, product_name
ORDER BY total_revenue DESC
LIMIT 5;

-- Revenue by product category
SELECT
    category,
    ROUND(SUM(revenue), 2)  AS total_revenue,
    SUM(quantity)           AS total_units_sold,
    COUNT(order_id)         AS order_count,
    COUNT(DISTINCT product_id) AS product_count
FROM fact
WHERE order_status = 'completed'
  AND category IS NOT NULL
GROUP BY category
ORDER BY total_revenue DESC;

-- Product margin analysis (revenue vs cost)
SELECT
    f.product_id,
    f.product_name,
    f.category,
    ROUND(SUM(f.revenue), 2)                    AS total_revenue,
    ROUND(SUM(f.quantity * p.unit_cost), 2)     AS total_cost,
    ROUND(SUM(f.revenue) - SUM(f.quantity * p.unit_cost), 2) AS gross_profit,
    ROUND(
        (SUM(f.revenue) - SUM(f.quantity * p.unit_cost))
        / NULLIF(SUM(f.revenue), 0),
        4
    )                                           AS gross_margin_rate
FROM fact f
JOIN products p ON f.product_id = p.product_id
WHERE f.order_status = 'completed'
GROUP BY f.product_id, f.product_name, f.category
ORDER BY gross_profit DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. CUSTOMER ANALYSIS
-- ─────────────────────────────────────────────────────────────────────────────

-- Revenue and orders by customer segment
SELECT
    segment,
    ROUND(SUM(revenue), 2)          AS total_revenue,
    COUNT(order_id)                 AS order_count,
    COUNT(DISTINCT customer_id)     AS unique_customers,
    ROUND(AVG(revenue), 2)          AS avg_order_value
FROM fact
WHERE order_status = 'completed'
  AND segment IS NOT NULL
GROUP BY segment
ORDER BY total_revenue DESC;

-- Top 10 customers by revenue
SELECT
    customer_id,
    customer_name,
    segment,
    ROUND(SUM(revenue), 2)      AS total_revenue,
    COUNT(order_id)             AS order_count
FROM fact
WHERE order_status = 'completed'
GROUP BY customer_id, customer_name, segment
ORDER BY total_revenue DESC
LIMIT 10;

-- Revenue by country
SELECT
    country,
    ROUND(SUM(revenue), 2)          AS total_revenue,
    COUNT(order_id)                 AS order_count,
    COUNT(DISTINCT customer_id)     AS unique_customers
FROM fact
WHERE order_status = 'completed'
  AND country IS NOT NULL
GROUP BY country
ORDER BY total_revenue DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. TIME-SERIES ANALYSIS
-- ─────────────────────────────────────────────────────────────────────────────

-- Monthly revenue trend
SELECT
    year_month,
    ROUND(SUM(revenue), 2)  AS total_revenue,
    COUNT(order_id)         AS order_count,
    ROUND(AVG(revenue), 2)  AS avg_order_value
FROM fact
WHERE order_status = 'completed'
GROUP BY year_month
ORDER BY year_month;

-- Month-over-month revenue growth (window function)
WITH monthly AS (
    SELECT
        year_month,
        ROUND(SUM(revenue), 2) AS total_revenue
    FROM fact
    WHERE order_status = 'completed'
    GROUP BY year_month
)
SELECT
    year_month,
    total_revenue,
    LAG(total_revenue) OVER (ORDER BY year_month) AS prev_month_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY year_month))
        / NULLIF(LAG(total_revenue) OVER (ORDER BY year_month), 0) * 100,
        2
    ) AS mom_growth_pct
FROM monthly
ORDER BY year_month;

-- Rolling 3-month average revenue
WITH monthly AS (
    SELECT
        year_month,
        ROUND(SUM(revenue), 2) AS total_revenue
    FROM fact
    WHERE order_status = 'completed'
    GROUP BY year_month
)
SELECT
    year_month,
    total_revenue,
    ROUND(
        AVG(total_revenue) OVER (
            ORDER BY year_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    ) AS rolling_3m_avg
FROM monthly
ORDER BY year_month;


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. OPERATIONAL HEALTH
-- ─────────────────────────────────────────────────────────────────────────────

-- Orders by status distribution
SELECT
    order_status,
    COUNT(*)                                AS order_count,
    ROUND(COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER (), 4) AS share
FROM orders
GROUP BY order_status
ORDER BY order_count DESC;

-- Orders without delivery (data quality check)
SELECT COUNT(*) AS orders_without_delivery
FROM orders o
LEFT JOIN deliveries d ON o.order_id = d.order_id
WHERE d.delivery_id IS NULL
  AND o.order_status NOT IN ('cancelled');
