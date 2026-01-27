# 🎬 TMDB Movie Data ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apache-airflow)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apache-spark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1-00ADD4?logo=delta)
![MinIO](https://img.shields.io/badge/MinIO-S3-C72C48?logo=minio)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker)

This project is an end-to-end (E2E) data engineering pipeline built using the **TMDB (The Movie Database)** dataset. It automates the extraction of raw data from GitHub, ingestion into a Data Lake (**MinIO**) in Parquet format, and transformation into **Delta Lake** tables using **Apache Spark**. The entire workflow is orchestrated by **Apache Airflow** running on Docker containers.

## 🏗️ Architecture & Workflow

The pipeline consists of three main stages:

1.  **Data Ingestion (Bronze Layer):**
    * A Python script fetches raw CSV files (Movies and Credits) directly from GitHub.
    * The data is processed in chunks to optimize memory usage and uploaded to a **MinIO** S3 bucket in `Parquet` format.
2.  **Data Transformation (Silver Layer):**
    * An **Apache Spark** job reads the raw data from the Bronze layer.
    * Complex JSON columns (such as cast, crew, genres) are parsed, exploded, and normalized.
    * The processed data is structured relationally and saved as **Delta Lake** tables.
3.  **Orchestration:**
    * **Apache Airflow** manages the workflow using `SSHOperator` to trigger tasks on a remote Spark Client container.

## 🚀 Tech Stack

* **Orchestration:** Apache Airflow
* **Data Processing:** Apache Spark (PySpark)
* **Storage & Data Lake:** MinIO (S3 Compatible Object Storage), Delta Lake
* **Language:** Python
* **Libraries:** `boto3`, `pandas`, `pyspark`, `delta-spark`

## 📂 Project Structure

```bash
.
├── dags/
│   └── tmdb_etl_dag.py        # Airflow DAG file (using SSHOperator)
├── scripts/
│   ├── data_ingestion.py      # Script for fetching data from GitHub to MinIO
│   └── transform_delta.py     # Spark script for Bronze -> Silver transformation
├── .env                       # Environment variables (MinIO credentials)
└── README.md                  # Project documentation
