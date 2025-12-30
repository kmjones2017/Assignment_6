SHOW VARIABLES LIKE 'hostname';
SELECT USER();

-- Used to delete duplicate 
SET FOREIGN_KEY_CHECKS = 0;
DELETE FROM order_items;
DELETE FROM orders;
DELETE FROM drug_batches;
DELETE FROM drug_formulations;
DELETE FROM raw_materials;
DELETE FROM suppliers;
DELETE FROM drugs;
DELETE FROM customers;
-- Global to connect to local_infile
SET GLOBAL local_infile = 1;

SHOW VARIABLES LIKE 'local_infile';

-- Pharmaceutical Manufacturing Schema
CREATE DATABASE IF NOT EXISTS pharma_db;

USE pharma_db;

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    customer_type ENUM('Hospital','Pharmacy','Clinic') NOT NULL,
    email VARCHAR(150),
    phone VARCHAR(20),
    address VARCHAR(255)
);

CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(150) NOT NULL,
    material_type VARCHAR(100),
    contact_email VARCHAR(150),
    country VARCHAR(100)
);

CREATE TABLE raw_materials (
    material_id INT PRIMARY KEY,
    material_name VARCHAR(150) NOT NULL,
    unit_of_measure VARCHAR(50),
    supplier_id INT,
    cost_per_unit DECIMAL(10,2),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

CREATE TABLE drugs (
    drug_id INT PRIMARY KEY,
    drug_name VARCHAR(150) NOT NULL,
    dosage_form VARCHAR(50),
    strength_mg INT,
    price DECIMAL(10,2)
);

CREATE TABLE drug_formulations (
    formulation_id INT PRIMARY KEY,
    drug_id INT NOT NULL,
    material_id INT NOT NULL,
    quantity_required DECIMAL(10,2),
    FOREIGN KEY (drug_id) REFERENCES drugs(drug_id),
    FOREIGN KEY (material_id) REFERENCES raw_materials(material_id)
);

CREATE TABLE drug_batches (
    batch_id INT PRIMARY KEY,
    drug_id INT NOT NULL,
    batch_number VARCHAR(100),
    manufacture_date DATE,
    expiration_date DATE,
    quantity_produced INT,
    FOREIGN KEY (drug_id) REFERENCES drugs(drug_id)
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
    order_item_id INT PRIMARY KEY,
    order_id INT NOT NULL,
    batch_id INT NOT NULL,
    quantity INT,
    unit_price DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (batch_id) REFERENCES drug_batches(batch_id)
);



SET FOREIGN_KEY_CHECKS = 1;

SHOW TABLES IN pharma_db;

-- Customers
LOAD DATA LOCAL INFILE 'C:/Users/pdraz/pharma_project/data/customers.csv'
INTO TABLE customers
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customer_id, name, customer_type, email, phone, address);

-- Suppliers
LOAD DATA LOCAL INFILE 'C:/Users/pdraz/pharma_project/data/suppliers.csv'
INTO TABLE suppliers
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(supplier_id, supplier_name, material_type, contact_email, country);

-- Raw Materials
LOAD DATA LOCAL INFILE 'C:/Users/pdraz/pharma_project/data/raw_materials.csv'
INTO TABLE raw_materials
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(material_id, material_name, unit_of_measure, supplier_id, cost_per_unit);

-- Drugs
LOAD DATA LOCAL INFILE 'C:/Users/pdraz/pharma_project/data/drugs.csv'
INTO TABLE drugs
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(drug_id, drug_name, dosage_form, strength_mg, price);

-- Drug Formulations
LOAD DATA LOCAL INFILE 'C:/Users/pdraz/pharma_project/data/drug_formulations.csv'
INTO TABLE drug_formulations
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(formulation_id, drug_id, material_id, quantity_required);

-- Drug Batches
LOAD DATA LOCAL INFILE 'C:/Users/pdraz/pharma_project/data/drug_batches.csv'
INTO TABLE drug_batches
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(batch_id, drug_id, batch_number, manufacture_date, expiration_date, quantity_produced);

-- Orders
LOAD DATA LOCAL INFILE 'C:/Users/pdraz/pharma_project/data/orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, customer_id, order_date);

-- Order Items
LOAD DATA LOCAL INFILE 'C:/Users/pdraz/pharma_project/data/order_items.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_item_id, order_id, batch_id, quantity, unit_price);


-- Verification Queries
-- Customers
SELECT * FROM pharma_db.customers;

-- Suppliers
SELECT * FROM pharma_db.suppliers;

-- Raw Materials
SELECT * FROM pharma_db.raw_materials;

-- Drugs
SELECT * FROM pharma_db.drugs;

-- Drug Formulations
SELECT * FROM pharma_db.drug_formulations;

-- Drug Batches
SELECT * FROM pharma_db.drug_batches;

-- Orders
SELECT * FROM pharma_db.orders;

-- Order Items
SELECT * FROM pharma_db.order_items;


--  DAY 7 TASKS: 1. Write batch insert logic 2. Use transactions 3. Add rollback on failure

--  1. Write batch insert logic

INSERT INTO customers (customer_id, name, customer_type, email, phone, address)
VALUES
    (7, 'HealthFirst Clinic', 'Clinic', 'contact@healthfirst.com', '555-111-2222', '12 Wellness Way'),
    (8, 'Metro Pharmacy', 'Pharmacy', 'info@metropharm.com', '555-333-4444', '88 City Center Blvd'),
    (9, 'CarePoint Medical', 'Hospital', 'admin@carepoint.com', '555-555-6666', '101 CarePoint Dr');


--  2. Write batch insert logic

START TRANSACTION;

INSERT INTO customers (customer_id, name, customer_type, email, phone, address)
VALUES (10, 'Harbor Clinic', 'Clinic', 'hello@harborclinic.com', '555-777-8888', '77 Harbor Rd');

INSERT INTO orders (order_id, customer_id, order_date)
VALUES (9, 10, '2024-06-01');


INSERT INTO order_items (order_item_id, order_id, batch_id, quantity, unit_price)
VALUES (10, 9, 1, 500, 9.99);

COMMIT;


-- 3. Add rollback on failure

DELIMITER $$

CREATE PROCEDURE insert_order_with_items()
BEGIN
    DECLARE exit handler for SQLEXCEPTION
    BEGIN
        ROLLBACK;
    END;

    START TRANSACTION;

    INSERT INTO orders (order_id, customer_id, order_date)
    VALUES (21, 1, '2024-06-02');

    INSERT INTO order_items (order_item_id, order_id, batch_id, quantity, unit_price)
    VALUES (31, 21, 999, 200, 12.50);  -- batch_id 999 does NOT exist → will trigger rollback

    COMMIT;
END$$

DELIMITER ;





--









--  


-- Data Aggregation

-- 1. Total Quantity per Order
SELECT 
    order_id,
    SUM(quantity) AS total_quantity
FROM order_items
GROUP BY order_id;



-- Average quantity size for orders
SELECT 
    AVG(quantity) AS avg_quantity_per_item
FROM order_items;


-- Total quantity ordered
SELECT 
    SUM(quantity) AS avg_quantity_per_item
FROM order_items;



-- Average quantity size and price for each order
SELECT 
    AVG(quantity * unit_price) AS avg_quantity_per_item
FROM order_items;



-- Total spent by each customers
SELECT 
    c.customer_id,
    c.name AS customer_name,
    SUM(oi.quantity * oi.unit_price) AS total_spent
FROM customers c -- c is abreviation for customer
JOIN orders o ON c.customer_id = o.customer_id -- o is for order
JOIN order_items oi ON o.order_id = oi.order_id -- oi is for order items
GROUP BY c.customer_id, c.name;



-- Sum spent by customer type
SELECT 
    c.customer_type,
    SUM(oi.quantity * oi.unit_price) AS total_spent
FROM customers c -- c is abreviation for customer
JOIN orders o ON c.customer_id = o.customer_id -- o is for order
JOIN order_items oi ON o.order_id = oi.order_id -- oi is for order items
GROUP BY c.customer_type;



-- Total Recenue by Product

SELECT 
    d.drug_id,
    d.drug_name,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM drugs d
JOIN drug_batches db ON d.drug_id = db.drug_id
JOIN order_items oi ON db.batch_id = oi.batch_id
GROUP BY d.drug_id, d.drug_name
ORDER BY total_revenue DESC;

-- Top Selling Products by Quantity and Revenue

SELECT 
    d.drug_id,
    d.drug_name,
    SUM(oi.quantity) AS total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM drugs d
JOIN drug_batches db ON d.drug_id = db.drug_id
JOIN order_items oi ON db.batch_id = oi.batch_id
GROUP BY d.drug_id, d.drug_name
ORDER BY total_quantity_sold DESC, total_revenue DESC;

