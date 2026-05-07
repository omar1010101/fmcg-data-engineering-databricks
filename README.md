# FMCG Data Engineering Pipeline with Databricks
## Project Overview

Following an [FMCG](https://en.wikipedia.org/wiki/Fast-moving_consumer_goods) sector acquisition, the *parent company* needed to integrate operational data from the acquired business (*child company*) into its existing analytics ecosystem. The challenge involved ingesting fragmented raw datasets, standardizing inconsistent data structures, and transforming them into reliable, business-ready insights for unified reporting and KPI monitoring.

This project solves that problem by building an end-to-end medallion lakehouse pipeline on *Databricks*. Raw data is ingested from *Amazon S3*, processed through *Bronze*, *Silver*, and *Gold* layers using *Delta Lake*, and served through executive dashboards to provide consolidated post-acquisition business analytics.

**Technologies Used**

* **Databricks**
* **Apache Spark**
* **PySpark**
* **AWS S3**
* **Delta Lake**
* **Databricks SQL**
* **Databricks Dashboard**
* **Python**

## Tech Stack

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


## Architecture Overview

The project follows a **medallion lakehouse architecture** implemented on Databricks to support scalable post-acquisition data integration and analytics.

### Pipeline Workflow

* **Raw Data Ingestion**

  * FMCG operational datasets are uploaded to **Amazon S3** as the centralized raw data storage layer.

* **Historical Batch Processing (Backfill)**

  * Historical datasets are processed in batch to establish the initial analytical baseline across all business entities.

* **Incremental Processing for Live Updates**

  * New incoming records are incrementally processed to continuously refresh analytical datasets with the latest operational changes.

* **Bronze Layer**

  * Stores raw ingested data in its original format for traceability, auditability, and historical preservation.

* **Silver Layer**

  * Performs data cleaning, standardization, schema validation, and transformation to produce structured and reliable intermediate datasets.

* **Workflow Orchestration**

  * Databricks workflows coordinate task dependencies and automate sequential execution of ingestion and transformation pipelines.

* **Gold Layer**

  * Generates curated analytical datasets optimized for business reporting and KPI analysis.

* **Denormalized Analytical Views**

  * Business-ready denormalized views are created to simplify querying and improve dashboard performance.

* **Dashboard Delivery**

  * Final datasets are served through **Databricks SQL dashboards**, enabling consolidated KPI monitoring and executive-level business insights across the integrated post-acquisition environment.
