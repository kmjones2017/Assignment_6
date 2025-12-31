-- =========================================================
-- Pharmaceutical Manufacturing Database Schema
-- =========================================================

-- Drop and recreate database 
DROP DATABASE IF EXISTS pharma_db;
CREATE DATABASE pharma_db;
USE pharma_db;

-- =========================================================
-- Core Reference Tables
-- =========================================================

DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    customer_type ENUM ('Hospital', 'Pharmacy', 'Clinic') NOT NULL,
    email VARCHAR(150),
    phone VARCHAR(20),
    address VARCHAR(255)
);

DROP TABLE IF EXISTS suppliers;
CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(150) NOT NULL,
    material_type VARCHAR(100),
    contact_email VARCHAR(150),
    country VARCHAR(100)
);

DROP TABLE IF EXISTS raw_materials;
CREATE TABLE raw_materials (
    material_id INT PRIMARY KEY,
    material_name VARCHAR(150) NOT NULL,
    unit_of_measure VARCHAR(50),
    supplier_id INT NOT NULL,
    cost_per_unit DECIMAL(10, 2) NOT NULL,
    CONSTRAINT fk_raw_materials_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES suppliers (supplier_id)
);

DROP TABLE IF EXISTS drugs;
CREATE TABLE drugs (
    drug_id INT PRIMARY KEY,
    drug_name VARCHAR(150) NOT NULL,
    dosage_form VARCHAR(50),
    strength_mg INT,
    price DECIMAL(10, 2) NOT NULL
);

-- =========================================================
-- Manufacturing Tables
-- =========================================================

DROP TABLE IF EXISTS drug_formulations;
CREATE TABLE drug_formulations (
    formulation_id INT PRIMARY KEY,
    drug_id INT NOT NULL,
    material_id INT NOT NULL,
    quantity_required DECIMAL(10, 2) NOT NULL,
    CONSTRAINT fk_formulations_drug
        FOREIGN KEY (drug_id)
        REFERENCES drugs (drug_id),
    CONSTRAINT fk_formulations_material
        FOREIGN KEY (material_id)
        REFERENCES raw_materials (material_id)
);

DROP TABLE IF EXISTS drug_batches;
CREATE TABLE drug_batches (
    batch_id INT PRIMARY KEY,
    drug_id INT NOT NULL,
    batch_number VARCHAR(100) NOT NULL UNIQUE,
    manufacture_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    quantity_produced INT NOT NULL,
    CONSTRAINT fk_batches_drug
        FOREIGN KEY (drug_id)
        REFERENCES drugs (drug_id)
);

-- =========================================================
-- Sales Tables
-- =========================================================

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers (customer_id)
);

DROP TABLE IF EXISTS order_items;
CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    batch_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id)
        REFERENCES orders (order_id),
    CONSTRAINT fk_order_items_batch
        FOREIGN KEY (batch_id)
        REFERENCES drug_batches (batch_id)
);
