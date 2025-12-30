## Pharmaceutical Manufacturing ETL Pipeline

This project implements an end-to-end **ETL (Extract, Transform, Load)** pipeline for a pharmaceutical manufacturing dataset. Raw CSV data is extracted and validated in Python, then loaded into a MySQL database as the system of record. From there, the data is transformed using SQL and pandas into analytics-ready datasets, which are then loaded into CSV outputs for reporting and validation.

---

## Installation Instructions (WIP)
   Include all steps required to install and set up the project (including dependencies, environment setup)
   - Python version 3.10 or above

   - Required libraries (pandas, mysql-connector-python, prefect, python-dotenv)

   - MySQL setup

   * Following the `.env.example` file's template, create a file titled `.env` (using Notepad) on the local machine that will be running the code. Next, replace the values for the username and password with the local machine's MySQL credentials. Then, place the new .env file in the root folder of the downloaded code repository. If successful, the code will reference said file when accessing the MySQL database throughout the pipeline.

   - Optional Prefect setup

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

## Running the System (WIP)
   Provide clear instructions on how to execute the Prefect flow and where to view the results.
   This section should explain:

   - How to run the SQL schema/load scripts

   - How to run the Prefect flow (or placeholder if still WIP)

   - How to run transformation and analytics scripts

   - Where outputs appear

---

## Tasks and Dependencies (WIP)
This section explains:

   - what each task does

   - how tasks depend on each other

   - how Prefect coordinates them

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

<img width="193" height="202" alt="image" src="https://github.com/user-attachments/assets/3a251a82-ab93-46fb-b6f0-c85662d57ffb" />

Shown above is the database as it appears in the left sidebar of MySQL Workbench.

---

## SQL Queries (WIP)

_All files are located in the db folder of the repository._

* **pharma_script.sql** - creates the database and tables...
* **pharma_schema_validation.sql** -  checks that all tables are present in the database, the table row counts match the CSVs, and displays the structure of each table
* **pharmadb_join_validation.sql** - checks for foreign key violations and includes aggregation queries to explain any missing joins
* **sample_queries.sql** - ...
* A description of Kevin's aggregation file(s)

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

The output of the script is shown below.
<img width="813" height="720" alt="image" src="https://github.com/user-attachments/assets/2f7412eb-a41a-42b7-98b8-7b06714f80c6" />

(The warning is simply recommending the use of SQLAlchemy because it is officially supported by pandas.)

### Validation & Sanity Checks

The analytics layer includes defensive checks such as:

* Null and negative value detection
* Revenue calculation consistency
* Aggregation reconciliation
* Valid customer segment classification

If any validation fails, execution stops immediately.

### Outputs

Analytics results are exported as CSV files located inside the repository under data/reports:

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

## Orchestration (Prefect) WIP

The pipeline is designed for orchestration using Prefect. Once integrated:

* Each stage will run as a Prefect task
* Failures will be visible in the Prefect UI
* Logs and validations will be centrally tracked
