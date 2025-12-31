USE pharma_db;

START TRANSACTION;

-- ==============================
-- Reference Tables
-- ==============================

INSERT INTO customers
    (customer_id, name, customer_type, email, phone, address)
VALUES
    (?, ?, ?, ?, ?, ?);

INSERT INTO suppliers
    (supplier_id, supplier_name, material_type, contact_email, country)
VALUES
    (?, ?, ?, ?, ?);

INSERT INTO raw_materials
    (material_id, material_name, unit_of_measure, supplier_id, cost_per_unit)
VALUES
    (?, ?, ?, ?, ?);

INSERT INTO drugs
    (drug_id, drug_name, dosage_form, strength_mg, price)
VALUES
    (?, ?, ?, ?, ?);

-- ==============================
-- Manufacturing Tables
-- ==============================

INSERT INTO drug_formulations
    (formulation_id, drug_id, material_id, quantity_required)
VALUES
    (?, ?, ?, ?);

INSERT INTO drug_batches
    (batch_id, drug_id, batch_number, manufacture_date, expiration_date, quantity_produced)
VALUES
    (?, ?, ?, ?, ?, ?);

-- ==============================
-- Sales Tables
-- ==============================

INSERT INTO orders
    (order_id, customer_id, order_date)
VALUES
    (?, ?, ?);

INSERT INTO order_items
    (order_item_id, order_id, batch_id, quantity, unit_price)
VALUES
    (?, ?, ?, ?, ?);

COMMIT;
