/*
BUSINESS SANITY CHECKS
Purpose:
- Detect logically impossible or suspicious values
- Operates ONLY on base tables
*/

USE pharma_db;

-- =========================
-- Quantity sanity
-- =========================
SELECT *
FROM order_items
WHERE quantity <= 0;

SELECT *
FROM drug_batches
WHERE quantity_produced <= 0;

-- =========================
-- Price sanity
-- =========================
SELECT *
FROM order_items
WHERE unit_price <= 0;

SELECT *
FROM drugs
WHERE price <= 0;

-- =========================
-- Date sanity
-- =========================
SELECT *
FROM orders
WHERE order_date > CURRENT_DATE
   OR order_date < '2000-01-01';

SELECT *
FROM drug_batches
WHERE expiration_date < manufacture_date;

-- =========================
-- NULL checks (physical columns only)
-- =========================
SELECT *
FROM orders
WHERE order_date IS NULL
   OR customer_id IS NULL;

SELECT *
FROM order_items
WHERE quantity IS NULL
   OR unit_price IS NULL
   OR order_id IS NULL
   OR batch_id IS NULL;