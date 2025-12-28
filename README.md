## Pharmaceutical Manufacturing ETL Pipeline

This project implements an end-to-end **ETL (Extract, Transform, Load)** pipeline for a pharmaceutical manufacturing dataset. The pipeline ingests raw operational data into a relational MySQL database, performs transformations using Python and pandas, and produces analytics-ready outputs with validation and integrity checks.

---

## Architecture Overview

**Data Flow Summary**

```
CSV Files
   ↓
MySQL Database (Normalized Schema)
   ↓
Python + pandas Transformations
   ↓
Analytics Outputs (CSV Reports)
```

### Technologies Used

* **MySQL** – relational data storage
* **Python** – transformation and analytics logic
* **pandas** – data manipulation and aggregation
* **Prefect** – orchestration and monitoring

---

## Upstream Ingestion & Load

Raw CSV files are ingested into a MySQL database using SQL-based loading mechanisms (e.g., `LOAD DATA INFILE`). This step establishes a normalized relational schema representing customers, suppliers, orders, products, batches, raw materials and formulations.

### Why Load into MySQL First?

Loading data into MySQL before transformation:

* Enforces **schema constraints and foreign keys**
* Provides a **single source of truth**
* Enables **data integrity validation** at the database level
* Reflects real-world data engineering workflows where analytics consume curated operational data rather than raw files

The MySQL database serves as the authoritative source for downstream transformations and analytics.

---

## Transformation Layer

The transformation step extracts data from MySQL and builds an analytics-ready fact table using pandas.

### Key Transformation Tasks

* Load source tables from MySQL
* Validate required columns
* Perform left joins across:

  * customers
  * orders
  * order_items
  * drug_batches
  * drugs
* Preserve lineage while handling missing joins safely
* Produce a consolidated “orders fact” dataset

Transformations are implemented in `transformation.py`.

---

## Analytics Layer

Analytics are performed in `analytics.py` using pandas.

### Analytics Objectives

1. **Total Revenue by Product**
2. **Top-Selling Products**

   * By total quantity sold
   * By total revenue
3. **Customer Segmentation**

   * High / Medium / Low spenders based on total order value
   * Based on the provided dataset, all customers fell below the medium-spend threshold. In a real production system, thresholds would be calibrated dynamically or based on historical percentiles.

### Validation & Sanity Checks

The analytics layer includes defensive checks such as:

* Null and negative value detection
* Revenue calculation consistency
* Aggregation reconciliation
* Valid customer segment classification

If any validation fails, execution stops immediately.

### Outputs

Analytics results are exported as CSV files located inside the data folder's reports folder:

* `revenue_by_product.csv`
* `top_products_by_quantity.csv`
* `top_products_by_revenue.csv`
* `customer_segmentation.csv`

---

## Error Handling & Data Quality

Error handling is implemented at multiple layers:

### Database Level

* Primary keys and foreign keys enforce referential integrity

### Transformation Level

* Column presence validation
* Safe left joins to prevent silent data loss

### Analytics Level

* Assertions ensure metric correctness
* Aggregation consistency checks prevent mismatched totals

When integrated into Prefect, failed assertions will cause task and flow failures, enabling monitoring and alerting.

---

## Orchestration (Prefect)

The pipeline is designed for orchestration using Prefect. Once integrated:

* Each stage will run as a Prefect task
* Failures will be visible in the Prefect UI
* Logs and validations will be centrally tracked
