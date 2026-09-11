# FMCG Data Engineering Pipeline 🏭

An end-to-end data engineering pipeline built on **Databricks** and **AWS S3**, processing FMCG (Fast-Moving Consumer Goods) sales data through a **Bronze → Silver → Gold** medallion architecture. The pipeline handles incremental loads, data quality enforcement, date standardisation, and Delta Lake merges — delivering clean, analytics-ready fact and dimension tables.

---

## Architecture Overview

```
AWS S3 (landing/)
      │
      ▼
┌─────────────┐
│   BRONZE    │  Raw ingestion — append only, schema evolution, Change Data Feed
└─────────────┘
      │
      ▼
┌─────────────┐
│   SILVER    │  Cleansed & standardised — type casting, date parsing,
│             │  deduplication, invalid record filtering, joins
└─────────────┘
      │
      ▼
┌─────────────┐
│    GOLD     │  Analytics-ready — fact tables, dimension tables,
│             │  Delta merge (upsert), aggregations
└─────────────┘
      │
      ▼
  Power BI / Reporting
```

---

## Project Structure

```
fmcg-pipeline/
│
├── 1_setup/
│   ├── 1_setup.py              # Catalog + schema creation (fmcg.bronze / silver / gold)
│   └── utilities.py            # Shared schema variables (bronze_schema, silver_schema, gold_schema)
│
├── 2_fact/
│   ├── 1_full_load_fact.py     # One-time historical load of orders into fact table
│   └── 2_incremental_load_fact.py  # Daily incremental load with Delta merge
│
├── 3_dimensions/
│   ├── 1_customer_data_processing.py   # Customer dimension — cleansing & Silver/Gold load
│   ├── 2_products_data_processing.py   # Products dimension — Bronze → Silver → Gold
│   ├── 3_pricing_data_processing.py    # Gross price dimension — multi-format date handling, merge
│   └── dim_date_table_creation.py      # Date dimension table generation
│
└── README.md
```

---

## Notebooks

### `1_setup/`

| Notebook | Purpose |
|---|---|
| `1_setup` | Creates the `fmcg` Unity Catalog and `bronze`, `silver`, `gold` schemas |
| `utilities` | Defines shared schema name variables used across all notebooks via `%run` |

---

### `2_fact/`

#### `1_full_load_fact` — Historical Full Load
- Reads all orders CSVs from `s3://sports-kingdom/orders/`
- Writes to `fmcg.bronze.orders` (overwrite)
- Applies silver transformations: invalid customer ID filtering, multi-format date parsing, deduplication
- Joins with `fmcg.silver.products` to enrich with `product_code`
- Writes to `fmcg.silver.orders` and `fmcg.gold.fact_orders`

#### `2_incremental_load_fact` — Daily Incremental Load
- Reads only new CSVs from `s3://sports-kingdom/orders/landing/`
- Appends raw data to `fmcg.bronze.orders`
- Writes incremental data to a bronze staging table for isolated transformation
- Moves processed files to `s3://sports-kingdom/orders/processed/` after successful write
- Silver transformations applied to staging only (not full bronze table)
- Delta **merge** (upsert) into `fmcg.silver.orders` and `fmcg.gold.sb_fact_orders`
- Aggregates order quantities and merges into parent `fmcg.gold.fact_orders`
- Staging tables dropped at end of run

---

### `3_dimensions/`

#### `1_customer_data_processing`
- Ingests customer CSV from S3
- Cleanses and standardises customer attributes
- Writes to `fmcg.bronze.customers` → `fmcg.silver.customers` → `fmcg.gold.dim_customers`

#### `2_products_data_processing`
- Ingests product master data from S3
- Applies transformations and deduplication
- Writes to `fmcg.bronze.products` → `fmcg.silver.products` → `fmcg.gold.dim_products`

#### `3_pricing_data_processing`
- Ingests gross price CSVs from S3 (`s3://sports-kingdom/gross_price/`)
- Handles multiple date formats using `try_to_timestamp` + `coalesce`
- Validates and corrects gross price values (negative → positive, non-numeric → 0)
- Joins with `fmcg.silver.products` to fetch `product_code`
- Deduplicates by `product_code`, `year`, `month`
- Writes to `fmcg.bronze.gross_price` → `fmcg.silver.gross_price` → `fmcg.gold.sb_dim_gross_price`
- Delta **merge** into parent `fmcg.gold.dim_gross_price`

#### `dim_date_table_creation`
- Generates a date dimension table covering the full date range of the dataset
- Fields include date, day, month, quarter, year, weekday, fiscal period
- Written to `fmcg.gold.dim_date`

---

## Key Technical Decisions

### Medallion Architecture
Data moves through three layers, with each layer serving a distinct purpose — raw storage (Bronze), cleansed data (Silver), and analytics-ready tables (Gold). This makes debugging straightforward: if something looks wrong in Gold, you can trace it back through Silver and Bronze.

### Incremental Load with Staging Tables
Rather than re-processing the entire Bronze table on every run, a staging table isolates only the newly arrived data. This keeps transformation compute proportional to the size of new data, not historical data.

### Delta Merge (Upsert)
Gold tables use `whenMatchedUpdate` + `whenNotMatchedInsert` rather than overwrite, preserving historical records and allowing reprocessing of corrected source files without duplication.

### Date Standardisation
Source files arrive with dates in multiple formats (`yyyy-MM-dd`, `MMMM d, yyyy`, `dd/MM/yyyy`, Unix timestamps, etc.). The pipeline uses `try_to_timestamp` inside `coalesce` to try each format in order, safely returning null on no match rather than erroring.

### Deduplication Before Merge
Source rows are deduplicated on the merge key before the merge runs, preventing the `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` error that occurs when multiple source rows match a single target row.

---

## Tech Stack

| Tool | Role |
|---|---|
| **Databricks** | Compute, notebook orchestration, Unity Catalog |
| **Apache Spark / PySpark** | Distributed data transformation |
| **Delta Lake** | ACID transactions, time travel, Change Data Feed |
| **AWS S3** | Raw file landing zone and processed archive |
| **SQL (Databricks SQL)** | Catalog and schema creation |

---

## Data Sources

| Source | Format | Load Type |
|---|---|---|
| Orders | CSV (daily files) | Incremental |
| Customers | CSV | Full load |
| Products | CSV | Full load |
| Gross Price | CSV | Full load + merge |

---

## Running the Pipeline

1. Run `1_setup/1_setup` once to create the catalog and schemas
2. Run dimension notebooks (`customer`, `products`, `pricing`, `dim_date`) first — fact tables join against them
3. Run `2_fact/1_full_load_fact` once for the historical backfill
4. Drop new daily CSVs into `s3://sports-kingdom/orders/landing/`
5. Run `2_fact/2_incremental_load_fact` for each new daily file
