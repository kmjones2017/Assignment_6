## Pharmaceutical Manufacturing ETL Pipeline

This project implements an end-to-end **ETL (Extract, Transform, Load)** pipeline for a pharmaceutical manufacturing dataset. Raw CSV data is extracted and validated in Python, then loaded into a MySQL database as the system of record. From there, the data is transformed using **both SQL and pandas** into analytics-ready datasets. The two independent analytics paths are cross-validated before trusted SQL outputs are persisted for reporting.

### Project Scope

This project is intentionally scoped for local execution and instructional clarity. It focuses on data correctness, validation, and orchestration patterns rather than production deployment concerns such as cloud infrastructure, automated scheduling, or CI/CD integration.

### Why This Project Exists

This project was created to demonstrate how an end-to-end data pipeline can be designed with correctness, validation, and observability as first-class concerns. Rather than focusing solely on data movement, the pipeline emphasizes schema enforcement, cross-engine analytics validation, and explicit failure handling — patterns commonly expected in real-world data engineering systems.

This dual-engine approach (SQL + pandas) ensures that persisted outputs are verified for correctness, providing a clear data quality signal before reporting.

---

## Quick Start

This section walks you through getting the pipeline running locally with minimal setup.

### 1. Prerequisites

* **Python 3.10+**
* **MySQL 8+** running locally
* **Git**

### 2. Clone the Repository

```bash
git clone --branch pipeline_flow_local_test https://github.com/kmjones2017/Assignment_6.git
cd Assignment_6
```

---

### 3. Create and Activate a Virtual Environment

```bash
python -m venv venv
```

Activate it:

* **Windows (CMD):** `venv\Scripts\activate`
* **Windows (PowerShell):** `./venv/Scripts/Activate.ps1`
* **macOS/Linux:** `source venv/bin/activate`

---

### 4. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

The `requirements.txt` includes:

* prefect
* pandas
* numpy
* mysql-connector-python
* python-dotenv

---

### 5. Configure Environment Variables

Create a `.env` file in the **project root** (based on `.env.example`):

```env
DB_HOST=localhost
DB_USER=your_mysql_user
DB_PASSWORD=your_mysql_password
DB_NAME=pharma_db
```

These credentials are used throughout the pipeline to connect to MySQL.

---

### 6. Start the Prefect Local Server (Optional but Recommended)

In one terminal (virtual environment activated):

```bash
prefect server start
```

This launches the Prefect UI at:

```
http://127.0.0.1:4200
```

The UI provides task-level visibility, logs, retries, and flow status.

---

### 7. Run the Pipeline

In **another terminal** (same virtual environment):

```bash
python -m flows.master_flow
```

---

## Architecture Overview

### Accurate Pipeline Diagram (Based on Code)

```text
┌──────────────────────────┐
│   Pharma ETL Master Flow │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│ Create MySQL Schema      │
│ (pharma_schema.sql)      │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│ Load CSV Data → MySQL    │
│ (load_all_csv_data)      │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│ Ingestion Sanity Checks  │
│ (pharma_ingestion_*)     │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│ Business Rule Checks     │
│ (sanity_checks.sql)      │
└─────────────┬────────────┘
              │
              ▼
      ┌─────────────────────────────┐
      │      Parallel Subflows       │
      └─────────────┬───────────────┘
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
┌──────────────────────┐  ┌──────────────────────┐
│ SQL Analytics Subflow│  │ Pandas Analytics      │
│ - orders_fact.sql    │  │ - build_orders_fact()│
│ - orders_aggregates  │  │ - pandas aggregations│
└───────────┬──────────┘  └───────────┬──────────┘
            │                         │
            └───────────┬─────────────┘
                        ▼
┌──────────────────────────┐
│ SQL ↔ Pandas Validation  │
│ (validate_sql_vs_pandas) │
└─────────────┬────────────┘
              │
              ▼
┌──────────────────────────┐
│ Persist SQL Outputs CSV  │
│ (data/reports/)          │
└──────────────────────────┘
```
### Why SQL + Pandas?

This pipeline intentionally computes analytics using **both SQL and pandas** to validate correctness across independent execution engines.

* **SQL** reflects how analytics are commonly performed in production data warehouses
* **pandas** provides a flexible, programmatic reference implementation
* Cross-engine validation reduces the risk of silent logic errors in joins, aggregations, or business rules

Rather than assuming a single implementation is correct, the pipeline treats agreement between engines as a data quality signal. Only SQL results that successfully reconcile with pandas are persisted as trusted outputs.

---

## Running the System

The pipeline is executed via a single Prefect flow entrypoint.

* Schema creation, ingestion, validation, analytics, and persistence are all orchestrated automatically
* Outputs are written to `data/reports/` as CSV files
* Failures at any stage halt execution and surface errors in the Prefect UI

---

### Prefect Flow Execution Timeline

Below is a screenshot of the **SQL Analytics subflow** run in the Prefect UI. This view shows exact task execution order, durations, and completion status.  

> **Note:** Prefect enforces task execution order and parallelism even if some tasks appear unconnected in the dependency graph. The timeline below provides the clearest representation of sequencing and task durations.

<img width="467" height="197" alt="Prefect SQL Analytics subflow timeline showing task order, durations, and completion" src="https://github.com/user-attachments/assets/7f1c240f-6692-47d8-aaff-f6c5e4faa91a" />

*Prefect task execution timeline for the SQL Analytics subflow; execution order and durations are enforced even if tasks appear unconnected in the dependency graph.*

---

## Tasks and Dependencies

This project uses Prefect to orchestrate task execution and enforce dependencies between pipeline stages. While individual tasks may rely on database mutations or file outputs rather than returned objects, execution order is strictly defined by the flow.

At a high level, the pipeline proceeds as follows:

1. **Schema Creation**
   * Executes `pharma_schema.sql` to create all required tables
   * Must succeed before any data ingestion occurs

2. **Data Ingestion**
   * Loads raw CSV files into normalized MySQL tables
   * Establishes the database as the system of record

3. **Sanity & Business Rule Checks**
   * Verifies row counts, referential integrity, and business constraints
   * Prevents downstream analytics from running on invalid data

4. **Analytics Subflows (Parallel Execution)**
   * **SQL Analytics Subflow**
     * Builds fact and aggregation datasets using SQL
   * **Pandas Analytics Subflow**
     * Independently builds equivalent datasets using pandas

5. **Cross-Engine Validation**
   * Compares SQL and pandas results to ensure metric parity
   * Fails the flow immediately if discrepancies are detected

6. **Persistence**
   * Writes trusted SQL analytics outputs to CSV files under `data/reports/`
   * Only runs after validation succeeds

Prefect handles retries, failure propagation, and execution visibility throughout the pipeline.

---

## Repository Structure

The repository is organized as follows, with each folder and key file annotated for purpose:

```text
Assignment_6/
├── flows/
│   ├── master_flow.py              # Orchestrates the full ETL pipeline
│   ├── sql_analytics_subflow.py    # SQL fact + aggregation logic
│   └── pandas_analytics_subflow.py # Pandas fact + aggregation logic
│
├── tasks/
│   ├── load_data.py                # CSV → MySQL ingestion
│   ├── sql_executor.py             # Safe SQL execution utility
│   ├── sanity_check_ingestion.py   # Ingestion + business checks
│   ├── validation.py               # SQL vs pandas reconciliation
│   └── persist_sql_outputs.py      # CSV export logic
│
├── db/
│   ├── pharma_schema.sql               # Creates the database and schema
│   ├── pharma_ingestion_validation.sql # Used to ensure referential integrity
│   ├── sanity_checks.sql               # Used to ensure business-rule compliance
│   ├── orders_fact.sql                 # Query for creating a fact table in SQL
│   └── orders_aggregates.sql           # SQL aggregation queries to be persisted
│
├── data/
│   ├── raw/                            # Raw CSV files used for ingestion
│   └── reports/                        # Final CSV outputs
│
├── docs/
│   └── data_dictionary.md              # Data dictionary describing tables and fields
│
├── .env.example                         # Example environment file for database connection credentials
├── requirements.txt                     # Python dependencies for running the pipeline
└── README.md                            # Project overview, instructions, and documentation
```

---

## Design Decisions

* **MySQL as system of record** to enforce schema and referential integrity
* **Dual analytics engines (SQL + pandas)** to validate correctness
* **Prefect orchestration** for retries, observability, and modularity
* **Explicit validation step** instead of assuming correctness

Alternative approaches (single-engine analytics, no orchestration, or direct CSV analytics) were intentionally avoided to better reflect real-world data engineering workflows.

---

## Configuration Notes

* File paths are currently hard-coded for simplicity and reproducibility. In a production system, these would be parameterized via environment variables or Prefect parameters to support multiple environments (local, CI, cloud storage).
* Scheduling is intentionally omitted in this implementation and can be added later using Prefect schedules or deployments.
* Validation tasks focus on correctness checks rather than rich reporting; enhanced logging or persisted validation artifacts would improve observability.

---

## Possible Improvements With More Time

This section captures reflective improvements beyond the scope of the project timeline:

* Parameterize dataset locations and database targets for multi-environment deployments
* Replace `mysql-connector-python` with **SQLAlchemy** for better compatibility and connection management
* Add structured validation outputs (diff tables, metrics, or persisted artifacts) instead of boolean pass/fail checks
* Replace `print` statements with structured logging for improved observability
* Introduce optional scheduling and retries via Prefect deployments
* Improve Prefect UI observability with task-level logging and artifacts
* Add automated tests (schema validation, transformation logic, and aggregation correctness)
* Add lightweight CI checks (linting, dry-run analytics)
