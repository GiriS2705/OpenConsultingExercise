## Customer Analytics Data Pipeline (Conceptual)

The diagram below illustrates how raw data flows into Snowflake, is transformed with DBT, and orchestrated by Airflow into a star schema for analytics:

![Customer Analytics Data Pipeline](docs/pipeline_diagram.png)

- **External Sources** → Snowflake RAW schema  
- **DBT Staging → Dim/Fact → Marts** layers  
- **Airflow** orchestrates ingestion, DBT runs, and advanced data quality checks  
- **Fact_Orders** joins with **Dim_Customers**, **Dim_Products**, and **Dim_Date** (Star Schema)
