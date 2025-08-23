# Customer Analytics Data Pipeline & Advanced Considerations

**Date:** 2025-08-22

## Conceptual Star Schema
```
                 Dim_Customers
                       |
Dim_Date  ----  Fact_Orders  ---- Dim_Products
```
- **Fact_Orders**: quantity, unit_price, total_amount, order_status, timestamps
- **Dim_Customers**: name, email, geography
- **Dim_Products**: name, category, brand, price
- **Dim_Date**: full calendar attributes

## AI-Generated Code & Minor Refinements
This repository contains:
- `sql/01_ddl_snowflake.sql` and `sql/02_inserts_snowflake.sql`
- `dbt/models/staging/` (`stg_customers.sql`, `stg_products.sql`, `stg_orders.sql`)
- `dbt/models/dim/`, `dbt/models/fact/`, `dbt/models/mart/`
- `airflow/dags/customer_analytics_dag.py`

**Refinements made:**
- Added Snowflake `CHECK` constraints and sensible defaults.
- Materialized marts as `table`, upstream as `view` for agility.
- Introduced macros for null-rate and non-negative checks.
- Defensive aggregations with `coalesce` to avoid null-driven undercounts.

## Advanced Data Quality Check & Imputation (10% Challenge)
### What it does
- **Anomaly detection**:
  - IQR on `quantity` (flags unusually low/high quantities).
  - Z-score on `total_amount` per `product_id` (flags outliers, |z| ≥ 3).
- **Referential integrity**: verifies `customer_id` and `product_id` exist in dims.
- **Action**: If issues are found, DAG **fails** with human-readable suggestions in logs.

### Logic flow (Airflow `PythonOperator`)
1. Query Snowflake for IQR outliers and z-score outliers.
2. Query for FK breaks via LEFT JOINs.
3. If any issues → print JSON summary and suggestions:
   - Recompute `TOTAL_AMOUNT = QUANTITY * UNIT_PRICE` or replace with product median.
   - Clamp `quantity` within IQR or route to manual review.
   - Quarantine rows with FK breaks until dims corrected.
4. Raise `AirflowException` to fail fast (toggleable).

### Imputation Strategy (where/when)
- **Staging**: *No destructive imputation* (keep raw semantics).
- **Dim/Fact views**: Allow simple `COALESCE` for modeling stability.
- **Marts**: Impute **only for aggregates** (e.g., recompute `total_amount` if null). Raw fact rows remain available for audit.

**Example (mart)**:
```sql
sum(coalesce(o.total_amount, coalesce(o.quantity,0) * coalesce(o.unit_price,0))) as total_sales
```

## DBT Staging Strategy
- `stg_*` models select from `RAW` schema, handling:
  - data types (`try_to_decimal`, `try_to_timestamp_ntz`),
  - normalization (`lower`, `initcap`, trimming),
  - basic null cleaning (`nullif`).
- Dim/Fact models read from staging; marts aggregate for BI.

`dbt_project.yml` defines schemas per layer and can be wired to your `profiles.yml` for Snowflake.

## Orchestration (Airflow)
- **ingest_raw**: simulates ingestion with a `BashOperator`.
- **create_tables/load_seed_data**: optional Snowflake seeds for demo.
- **dbt_run**: runs transformations (swap with real command or a mock).
- **dq_advanced**: advanced DQ (IQR, z-score, FK checks) with remediation suggestions.
- **done**: logs completion.

## Hybrid Data Ingestion Strategy
- **Batch OLTP → Snowflake**: 
  - *AWS*: RDS snapshots → S3 → Snowpipe (Auto-Ingest) + Glue for cataloging.
  - *Azure*: SQL MI or Synapse Pipelines → ADLS Gen2 → Snowflake External Stage + Auto-Copy.
  - *GCP*: Cloud SQL → GCS → Snowpipe + Dataflow for transforms when needed.
- **Streaming clicks**:
  - Web → Kafka/Kinesis/Event Hubs → Snowflake **Snowpipe Streaming** for low-latency landings; optional Flink/Spark for sessionization before landing.
- **Support tickets (SaaS)**:
  - Fivetran/Stitch/Hevo → Snowflake RAW schema (EL). Rate-limited APIs via Airflow micro-batches when connectors unavailable.
- **Freshness & cost**:
  - Use small “X-Small” virtual warehouses for micro-batches; scale to M/L only for dbt marts.
  - Task orchestration with backfills; SLA monitoring via Airflow SLAs and dbt source freshness.

## AI Usage Notes
**Effective prompts:**
- “Generate Snowflake DDL for dims customers/products/date and fact_orders with constraints and dummy data.”
- “Write dbt staging models selecting from RAW.* with light standardization.”
- “Airflow DAG that simulates ingestion, runs dbt, and adds advanced DQ with IQR/z-score.”

**Ineffective prompts:**
- “Make a pipeline” (too broad; generated vague snippets).
- “Add quality checks” (needed specifics like IQR/z-score, FK checks).

**Critical thinking (10%):**
- Snowflake FKs are informational → enforced DQ in dbt tests and Airflow runtime checks.
- Outlier logic uses robust IQR for skewed qty and z-scores for amount by product.
- Imputation limited to marts to avoid corrupting raw facts.

## Tools Used
- **AI**: ChatGPT
- **Warehouse**: Snowflake
- **Transform**: dbt
- **Orchestration**: Apache Airflow
- **EL Connectors**: (conceptual) Fivetran/Stitch/Hevo
- **Messaging/Streaming**: Kafka/Kinesis/Event Hubs
- **Object Storage**: S3 / ADLS / GCS
- **Version Control**: Git/GitHub

## Getting Started
```bash
# Snowflake
!source sql/01_ddl_snowflake.sql
!source sql/02_inserts_snowflake.sql

# dbt (example)
cd dbt && dbt run && dbt test

# Airflow (mount repo at /opt/airflow/dags)
airflow dags trigger customer_analytics_pipeline
```

**Share** your private GitHub repo with **@OpenConsulting**.
