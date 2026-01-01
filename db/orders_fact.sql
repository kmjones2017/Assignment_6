-- orders_fact.sql
-- Business-ready fact dataset (no aggregation)
-- Mirrors build_orders_fact_pandas()

SELECT
    o.order_id,
    o.order_date,
    o.customer_id,

    c.name AS customer_name,

    oi.batch_id,
    oi.quantity,
    oi.unit_price,

    db.drug_id,
    d.drug_name

FROM orders o

LEFT JOIN customers c
    ON o.customer_id = c.customer_id

LEFT JOIN order_items oi
    ON o.order_id = oi.order_id

LEFT JOIN drug_batches db
    ON oi.batch_id = db.batch_id

LEFT JOIN drugs d
    ON db.drug_id = d.drug_id;