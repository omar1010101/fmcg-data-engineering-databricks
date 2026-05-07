# FMCG Data Engineering Pipeline with Databricks
## Project Overview

Following an [FMCG](https://en.wikipedia.org/wiki/Fast-moving_consumer_goods) sector acquisition, the *parent company* needed to integrate operational data from the acquired business (*child company*) into its existing analytics ecosystem. The challenge involved ingesting fragmented raw datasets, standardizing inconsistent data structures, and transforming them into reliable, business-ready insights for unified reporting and KPI monitoring.

This project solves that problem by building an end-to-end medallion lakehouse pipeline on *Databricks*. Raw data is ingested from *Amazon S3*, processed through *Bronze*, *Silver*, and *Gold* layers using *Delta Lake*, and served through executive dashboards to provide consolidated post-acquisition business analytics.

