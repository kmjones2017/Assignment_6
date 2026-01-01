/*
PHARMA INGESTION VALIDATION
Purpose:
- Verify schema presence
- Confirm table population
- Detect referential integrity violations
*/

USE pharma_db;

-- =========================
-- Schema Presence
-- =========================
SHOW TABLES;

-- =========================
-- Row Count Sanity
-- (Presence only, not correctness)
-- =========================
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers;
SELECT 'suppliers' AS table_name, COUNT(*) AS row_count FROM suppliers;
SELECT 'raw_materials' AS table_name, COUNT(*) AS row_count FROM raw_materials;
SELECT 'drugs' AS table_name, COUNT(*) AS row_count FROM drugs;
SELECT 'drug_formulations' AS table_name, COUNT(*) AS row_count FROM drug_formulations;
SELECT 'drug_batches' AS table_name, COUNT(*) AS row_count FROM drug_batches;
SELECT 'orders' AS table_name, COUNT(*) AS row_count FROM orders;
SELECT 'order_items' AS table_name, COUNT(*) AS row_count FROM order_items;

-- =========================
-- Schema Definitions (PK/FK visibility)
-- =========================
SHOW CREATE TABLE customers;
SHOW CREATE TABLE suppliers;
SHOW CREATE TABLE raw_materials;
SHOW CREATE TABLE drugs;
SHOW CREATE TABLE drug_formulations;
SHOW CREATE TABLE drug_batches;
SHOW CREATE TABLE orders;
SHOW CREATE TABLE order_items;

-- =========================
-- Referential Integrity Checks
-- Any returned rows = violation
-- =========================

-- Orders with missing customers
SELECT o.order_id, o.customer_id
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Order items without orders
SELECT oi.order_item_id, oi.order_id
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Order items without valid drug batches
SELECT oi.order_item_id, oi.batch_id
FROM order_items oi
LEFT JOIN drug_batches db ON oi.batch_id = db.batch_id
WHERE db.batch_id IS NULL;

-- Drug batches without valid drugs
SELECT db.batch_id, db.drug_id
FROM drug_batches db
LEFT JOIN drugs d ON db.drug_id = d.drug_id
WHERE d.drug_id IS NULL;

-- Drug formulations without valid drugs
SELECT df.formulation_id, df.drug_id
FROM drug_formulations df
LEFT JOIN drugs d ON df.drug_id = d.drug_id
WHERE d.drug_id IS NULL;

-- Drug formulations with missing raw materials
SELECT df.formulation_id, df.material_id
FROM drug_formulations df
LEFT JOIN raw_materials rm ON df.material_id = rm.material_id
WHERE rm.material_id IS NULL;

-- Raw materials without suppliers
SELECT rm.material_id, rm.supplier_id
FROM raw_materials rm
LEFT JOIN suppliers s ON rm.supplier_id = s.supplier_id
WHERE s.supplier_id IS NULL;