End-to-End Data Engineering Pipeline using Azure ADF, ADLS & Databricks
Project Overview

This project demonstrates an end-to-end data engineering pipeline built using Azure Data Factory, Azure SQL Database, Azure Data Lake Storage, and Databricks following the Medallion Architecture (Bronze, Silver, Gold).

The goal was to ingest raw source data, process it incrementally, and transform it into analytics-ready Gold layer dimension tables for downstream reporting and analysis.

Business Problem

The business receives raw data files from source systems and needs a scalable, incremental, and reliable pipeline to:

Load data into a structured database

Process only new or changed data

Build clean, analytics-ready datasets

Support downstream BI and analytical use cases

Architecture Overview

Data Flow:

Source Files
   ↓
Azure Data Factory
   ↓
Azure SQL Database
   ↓ (Incremental Load using Watermark)
Azure Data Lake Storage (Bronze)
   ↓
Databricks (Spark)
   ↓
Silver Layer (Cleaned & Transformed)
   ↓
Gold Layer (Dimension Tables)

Tech Stack

Azure Data Factory (ADF) – Orchestration & incremental pipelines

Azure SQL Database – Intermediate structured storage

Azure Data Lake Storage (ADLS Gen2) – Bronze layer storage

Azure Databricks – Data processing using PySpark

Apache Spark – Distributed transformations

Delta / Parquet – Storage format (as applicable)

Data Pipeline Breakdown
1. Source → Azure SQL Database (ADF)

Raw files ingested from source systems

Azure Data Factory pipelines used to load data into SQL tables

Ensured schema consistency and structured storage

2. Incremental Load: SQL → ADLS Bronze (ADF)

Implemented incremental data loading using Watermark Table

Only new or updated records were processed

Data stored in Bronze layer in ADLS (raw but structured)

Key Concepts:

Watermark-based incremental load

Reduced data movement and processing cost

Production-style ingestion pattern

3. ADLS Bronze → Silver Layer (Databricks)

Connected Databricks to ADLS

Read Bronze layer data into Spark DataFrames

Applied: Data cleaning

Stored refined datasets as Silver layer

4. Silver → Gold Layer (Databricks)

Built Gold layer dimension tables

Applied:

Business transformations

Joins and aggregations

Analytical modeling

Created analytics-ready datasets for downstream consumption

Medallion Architecture Implementation
Layer	Purpose
Bronze	Raw incremental data from SQL
Silver	Cleaned and transformed datasets
Gold	Business-ready dimension tables

This layered approach improves data quality, maintainability, and scalability.

Notebooks Structure
notebooks/
├── silver.py
├── goldDim_SalesRegion.py
├── goldDim_product.py
├── goldDim_date.py
├── goldDim_customer.py
└── fact_table.py

Each notebook represents a logical stage of the pipeline, similar to production data workflows.

Key Skills Demonstrated

End-to-end data pipeline design

Incremental data loading using watermarking

Medallion architecture (Bronze, Silver, Gold)

Spark DataFrame transformations

Databricks + ADLS integration

Azure Data Factory orchestration

Analytics-ready data modeling

Outcome

Built a scalable and incremental data pipeline

Reduced redundant data processing using watermark logic

Produced Gold layer dimension tables suitable for BI and analytics

Followed industry-standard data engineering architecture patterns
