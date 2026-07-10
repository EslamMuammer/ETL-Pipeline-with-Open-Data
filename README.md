# Olist Data Engineering Pipeline

An end-to-end **Data Engineering** project built on the **Brazilian Olist E-commerce Dataset**, demonstrating the complete analytics lifecycle from raw data ingestion to interactive business dashboards.

The project follows modern Data Engineering best practices by combining **Python**, **DuckDB**, **dbt**, **Apache Airflow**, and **Power BI** into a single automated pipeline.

---

# Project Goals

This project was designed to simulate a real-world analytics engineering workflow by:

- Cleaning and validating raw datasets
- Transforming data into analytics-ready models
- Building a dimensional data warehouse
- Automating the pipeline using Apache Airflow
- Creating interactive business dashboards
- Following modular and scalable Data Engineering practices

---

# Tech Stack

| Category | Technologies |
|------------|--------------|
| Programming | Python |
| Data Cleaning | Pandas, NumPy |
| Database | DuckDB |
| SQL | DuckDB SQL |
| Analytics Engineering | dbt Core |
| Orchestration | Apache Airflow |
| Data Visualization | Power BI |
| Version Control | Git & GitHub |
| Development Environment | VS Code + WSL Ubuntu |

---

# Project Architecture

```
                    Raw CSV Files
                           │
                           ▼
                Data Cleaning (Python)
                           │
                           ▼
                  Cleaned Data Files
                           │
                           ▼
                     DuckDB Database
                           │
                           ▼
               dbt Transformations
       (Staging → Intermediate → Marts)
                           │
                           ▼
                 Data Quality Testing
                           │
                           ▼
               Apache Airflow Pipeline
                           │
                           ▼
                 Power BI Dashboard
```

---

# Repository Structure

```
Olist/
│
├── airflow/
│   ├── dags/
│   ├── logs/
│   └── plugins/
│
├── cleaning/
│   ├── notebooks/
│   └── scripts/
│
├── models/
│   ├── staging/
│   ├── intermediate/
│   └── marts/
│
├── macros/
├── analyses/
├── snapshots/
├── tests/
├── seeds/
├── logs/
├── target/
│
├── Olist.duckdb
├── dbt_project.yml
├── profiles.yml
├── requirements.txt
└── README.md
```

---

# Data Pipeline

## 1. Data Collection

The project uses the **Olist Brazilian E-commerce Dataset**, containing:

- Customers
- Orders
- Order Items
- Products
- Sellers
- Payments
- Reviews
- Product Categories

---

## 2. Data Cleaning

The raw CSV files are cleaned using Python before loading into the warehouse.

Cleaning operations include:

- Handling missing values
- Removing duplicates
- Standardizing column names
- Correcting data types
- Formatting dates
- Removing invalid records
- Data validation

---

## 3. Data Warehouse

The cleaned datasets are loaded into **DuckDB**, which serves as the analytical database.

DuckDB was selected because it offers:

- Fast analytical performance
- Lightweight deployment
- Excellent SQL support
- Seamless integration with dbt

---

## 4. Analytics Engineering (dbt)

The warehouse is transformed using **dbt** following a layered architecture.

### Staging Layer

Responsibilities:

- Rename columns
- Standardize naming conventions
- Cast data types
- Clean raw fields

Models:

- stg_customers
- stg_orders
- stg_order_items
- stg_products
- stg_payments
- stg_reviews
- stg_sellers

---

### Intermediate Layer

Responsibilities:

- Join staging models
- Apply business logic
- Create reusable transformations

Models:

- int_olist

---

### Mart Layer

The final analytics-ready models.

#### Fact Tables

- fct_order_items

#### Dimension Tables

- dim_customers
- dim_products
- dim_orders_details
- dim_payments_summary
- dim_reviews
- dim_sellers

---

# Data Quality

The project includes automated data quality testing using dbt.

Implemented tests include:

- NOT NULL
- UNIQUE

Future enhancements:

- Relationships
- Accepted Values
- Freshness Tests
- Custom Generic Tests

---

# Data Model

```
                 dim_customers
                       │
                       │
dim_products ─── fct_order_items ─── dim_sellers
                       │
                       │
             dim_orders_details
                       │
                       │
             dim_payments_summary
                       │
                       │
                 dim_reviews
```

---

# Workflow Automation

The entire pipeline is orchestrated using **Apache Airflow**.

Pipeline Steps:

1. Load raw datasets
2. Execute Python cleaning scripts
3. Load cleaned data into DuckDB
4. Execute dbt seed
5. Execute dbt run
6. Execute dbt test
7. Refresh reporting layer

This allows the entire workflow to run automatically without manual intervention.

---

# Business Intelligence

Power BI connects directly to the transformed warehouse.

The dashboard provides insights including:

- Revenue Analysis
- Sales Trends
- Customer Segmentation
- Product Performance
- Seller Performance
- Payment Analysis
- Order Status Analysis
- KPI Overview

---

# Running the Project

## Clone Repository

```bash
git clone https://github.com/yourusername/Olist.git

cd Olist
```

---

## Create Virtual Environment

```bash
python -m venv .venv
```

Linux / WSL

```bash
source .venv/bin/activate
```

Windows

```bash
.venv\Scripts\activate
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Verify dbt Installation

```bash
dbt debug
```

---

## Install dbt Packages

```bash
dbt deps
```

---

## Build the Entire Project

```bash
dbt build
```

---

## Generate Documentation

```bash
dbt docs generate
```

---

## Launch Documentation

```bash
dbt docs serve
```

---

## Start Apache Airflow

```bash
airflow standalone
```

---

# Project Highlights

- End-to-End Data Engineering Pipeline
- Modular dbt Project Structure
- Data Cleaning with Python
- Automated Data Validation
- Dimensional Data Modeling
- Analytics Engineering Best Practices
- Apache Airflow Workflow Automation
- Interactive Power BI Dashboard
- Version Controlled with Git

---

# Skills Demonstrated

- Python
- SQL
- DuckDB
- dbt Core
- Apache Airflow
- ETL / ELT
- Data Cleaning
- Data Modeling
- Dimensional Modeling
- Data Validation
- Workflow Automation
- Business Intelligence
- Power BI
- Git
- GitHub


---

# Dataset

The project is based on the public **Brazilian Olist E-commerce Dataset**, one of the most widely used datasets for analytics engineering and business intelligence projects.
