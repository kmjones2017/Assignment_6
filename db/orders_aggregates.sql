-- orders_aggregates.sql
-- Aggregations built from orders_fact
-- Mirrors analytics.py Pandas logic

-- ============================
-- Revenue by Product
-- ============================

WITH orders_fact AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.name AS customer_name,
        oi.quantity,
        oi.unit_price,
        db.drug_id,
        d.drug_name
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN drug_batches db ON oi.batch_id = db.batch_id
    LEFT JOIN drugs d ON db.drug_id = d.drug_id
)

SELECT
    drug_id,
    drug_name,
    SUM(quantity) AS quantity_sold,
    SUM(quantity * unit_price) AS total_revenue
FROM orders_fact
GROUP BY drug_id, drug_name;

-- ============================
-- Top products by quantity
-- ============================

WITH orders_fact AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.name AS customer_name,
        oi.quantity,
        oi.unit_price,
        db.drug_id,
        d.drug_name
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN drug_batches db ON oi.batch_id = db.batch_id
    LEFT JOIN drugs d ON db.drug_id = d.drug_id
)

SELECT *
FROM (
    SELECT
        drug_id,
        drug_name,
        SUM(quantity) AS quantity_sold,
        SUM(quantity * unit_price) AS total_revenue
    FROM orders_fact
    GROUP BY drug_id, drug_name
) AS revenue_by_product
ORDER BY quantity_sold DESC
LIMIT 10;
 
-- ============================
-- Top products by revenue
-- ============================

WITH orders_fact AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.name AS customer_name,
        oi.quantity,
        oi.unit_price,
        db.drug_id,
        d.drug_name
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN drug_batches db ON oi.batch_id = db.batch_id
    LEFT JOIN drugs d ON db.drug_id = d.drug_id
)

SELECT *
FROM (
    SELECT
        drug_id,
        drug_name,
        SUM(quantity) AS quantity_sold,
        SUM(quantity * unit_price) AS total_revenue
    FROM orders_fact
    GROUP BY drug_id, drug_name
) AS revenue_by_product
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================
-- Customer Segmentation
-- ============================

WITH orders_fact AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.name AS customer_name,
        oi.quantity,
        oi.unit_price,
        db.drug_id,
        d.drug_name
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN drug_batches db ON oi.batch_id = db.batch_id
    LEFT JOIN drugs d ON db.drug_id = d.drug_id
)

SELECT
    customer_id,
    customer_name,
    SUM(quantity * unit_price) AS total_spent,
    COUNT(DISTINCT order_id) AS order_count,
    CASE
        WHEN SUM(quantity * unit_price) >= 50000 THEN 'High'
        WHEN SUM(quantity * unit_price) >= 20000 THEN 'Medium'
        ELSE 'Low'
    END AS segment
FROM orders_fact
GROUP BY customer_id, customer_name;