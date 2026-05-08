# FMCG Data Engineering Pipeline with Databricks
## Project Overview

Following an [FMCG](https://en.wikipedia.org/wiki/Fast-moving_consumer_goods) (Fast-moving consumer goods) sector acquisition, the *parent company* needed to integrate operational data from the acquired business (*child company*) into its existing analytics ecosystem. The challenge involved ingesting fragmented raw datasets, standardizing inconsistent data structures, and transforming them into reliable, business-ready insights for unified reporting and KPI monitoring.

This project solves that problem by building an end-to-end medallion lakehouse pipeline on *Databricks*. Raw data is ingested from *Amazon S3*, processed through *Bronze*, *Silver*, and *Gold* layers using *Delta Lake*, and served through executive dashboards to provide consolidated post-acquisition business analytics.



---


## Architecture

<img width="1535" height="686" alt="architecture-databricks" src="https://github.com/user-attachments/assets/06b8c75f-fc16-4a6e-b267-fa058152cc2a" />


The project follows a **medallion lakehouse architecture** implemented on Databricks to support scalable post-acquisition data integration and analytics.

### Pipeline Workflow

1. **Raw Data Ingestion** → FMCG operational datasets are uploaded to **Amazon S3** as the centralized raw data storage layer.

2. **Historical Batch Processing (Backfill)** → Historical datasets are processed in batch to establish the initial analytical baseline across all business entities.

3. **Incremental Processing for Live Updates** → New incoming records are incrementally processed to continuously refresh analytical datasets with the latest operational changes.

4. **Bronze Layer** → Stores raw ingested data in its original format for traceability, auditability, and historical preservation.

5. **Silver Layer** → Performs data cleaning, standardization, schema validation, and transformation to produce structured and reliable intermediate datasets.

6. **Workflow Orchestration** → Databricks workflows coordinate task dependencies and automate sequential execution of ingestion and transformation pipelines.

7. **Gold Layer** → Generates curated analytical datasets optimized for business reporting and KPI analysis.

8. **Denormalized Analytical Views** → Business-ready denormalized views are created to simplify querying and improve dashboard performance.

9. **Dashboard Delivery** → Final datasets are served through **Databricks SQL dashboards**, enabling consolidated KPI monitoring and executive-level business insights across the integrated post-acquisition environment.


---


## Tech Stack
<!--
* **Databricks**
* **Apache Spark**
* **PySpark**
* **AWS S3**
* **Delta Lake**
* **Databricks SQL**
* **Databricks Dashboard**
* **Python**
-->
| Category                   | Technology           | Purpose                                                |
| -------------------------- | -------------------- | ------------------------------------------------------ |
| **Cloud Storage**          | AWS S3               | Stores raw and processed datasets                      |
| **Data Platform**          | Databricks           | Unified environment for data engineering and analytics |
| **Distributed Processing** | Apache Spark         | Large-scale data transformation and processing         |
| **Storage Layer**          | Delta Lake           | Transactional storage for medallion architecture       |
| **Programming Language**   | Python / PySpark     | ETL pipeline development                               |
| **Query Engine**           | Databricks SQL       | Analytical querying and data serving                   |
| **Visualization**          | Databricks Dashboard | Executive KPI monitoring and reporting                 |
| **Architecture Pattern**   | Medallion Lakehouse  | Multi-layer data processing and refinement             |



---


## Dashboard & Business Insights

<img height="1000" alt="dashboard" src="https://github.com/user-attachments/assets/ac69ce9d-089b-4a73-a890-874802c059de" />


---


## Repository Structure

```bash
fmcg-data-engineering-databricks/
├── notebooks/
│   ├── 1_setup/
│   │   ├── dim_date_table_creation.py
│   │   ├── setup_catalogs.py
│   │   └── utilities.py
│   ├── 2_dimension_data_processing/
│   │   ├── 1_customers_data_processing.py
│   │   ├── 2_products_data_processing.py
│   │   └── 3_pricing_data_processing.py
│   └── 3_fact_data_processing/
│       ├── 1_full_load_fact.py
│       └── 2_incremental_load_fact.py
└── README.md
```

> **Note:** All files in the `notebooks/` directory are exported Databricks source notebooks provided in Python script format for improved readability.
---


## 📫 Connect with Me

- **Author:** *Omar EL KALKHA*
- **LinkedIn:** [https://www.linkedin.com/in/omar-el-kalkha/](https://www.linkedin.com/in/omar-el-kalkha/)
- **Email:** [omarelkalkha5@gmail.com](mailto:omarelkalkha5@gmail.com)

