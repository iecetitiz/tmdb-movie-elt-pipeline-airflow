# 🎬 TMDB Movie Data ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apache-airflow)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apache-spark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.1-00ADD4?logo=delta)
![MinIO](https://img.shields.io/badge/MinIO-S3-C72C48?logo=minio)
![Docker](https://img.shields.io/badge/Docker-Container-2496ED?logo=docker)

This project is an end-to-end (E2E) data engineering pipeline built using the **TMDB (The Movie Database)** dataset. It automates the extraction of raw data from GitHub, ingestion into a Data Lake (**MinIO**) in Parquet format, and transformation into **Delta Lake** tables using **Apache Spark**. The entire workflow is orchestrated by **Apache Airflow** running on Docker containers.

## 🚀 Project Overview

This project implements a scalable **Data Lakehouse** architecture to process TMDB movie data. It automates the end-to-end data lifecycle, transforming raw CSV files hosted on GitHub into optimized **Delta Lake** tables stored in **MinIO**.

The pipeline is designed to simulate a real-world production environment, featuring:
* **Ingestion:** Efficient fetching and chunking of raw data into a Bronze Layer (Parquet).
* **Transformation:** Complex schema parsing, normalization, and cleaning using Apache Spark to create a Silver Layer (Delta).
* **Orchestration:** Fully automated workflow management using Apache Airflow.

### 🏗️ Pipeline Architecture

![Architecture Flowchart](https://github.com/user-attachments/assets/2bb4b52b-cd0f-4f59-8f1c-9351bbb7f2a3)

## 📂 Dataset Information

The pipeline processes the **TMDB 5000 Movie Dataset**. You can browse the source folder containing the datasets directly on GitHub:

👉 **[View Dataset Folder](https://github.com/harshitcodes/tmdb_movie_data_analysis/tree/master/tmdb-5000-movie-dataset)**

The ETL process is configured to fetch data automatically from these specific raw files:

### 1. Movies Dataset
* **Source:** [`tmdb_5000_movies.csv`](https://raw.githubusercontent.com/harshitcodes/tmdb_movie_data_analysis/refs/heads/master/tmdb-5000-movie-dataset/tmdb_5000_movies.csv)
* **Content:** Metadata for ~5,000 movies including `budget`, `revenue`, `title`, `runtime`, and semi-structured JSON columns like `genres` and `production_companies`.

### 2. Credits Dataset
* **Source:** [`tmdb_5000_credits.csv`](https://raw.githubusercontent.com/harshitcodes/tmdb_movie_data_analysis/refs/heads/master/tmdb-5000-movie-dataset/tmdb_5000_credits.csv)
* **Content:** Cast and crew information linked by `movie_id`, containing heavy JSON arrays for `cast` and `crew`.

> **Note:** The pipeline downloads these files directly into the **Bronze Layer (MinIO)** using the `data_ingestion.py` script.
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

## 📊 Data Modeling

The pipeline transforms the raw `tmdb_5000_movies` and `tmdb_5000_credits` datasets into the following **Delta Tables** in the Silver Layer.

The ETL process normalizes semi-structured JSON arrays into relational tables to enable efficient querying:

| Table Name | Description | Source Column |
| :--- | :--- | :--- |
| **movies** | Core movie attributes (budget, revenue, runtime, etc.) | `tmdb_5000_movies.csv` |
| **crew** | Exploded crew members with department and job roles | `credits.crew` (JSON) |
| **cast** | Exploded cast members with character names | `credits.cast` (JSON) |
| **genre** | Movie genres mapping (One-to-Many) | `movies.genres` (JSON) |
| **keywords** | Plot keywords mapping | `movies.keywords` (JSON) |
| **production_companies** | Production companies involved | `movies.production_companies` (JSON) |
| **production_countries** | Countries where the movie was produced | `movies.production_countries` (JSON) |
| **spoken_languages** | Languages spoken in the movie | `movies.spoken_languages` (JSON) |


## ⚙️ Build & Run

You can spin up the entire infrastructure.

```bash
# 1. Create a minimal .env file with custom credentials
echo -e "AIRFLOW_UID=50000\nMINIO_ACCESS_KEY=minio_access_key\nMINIO_SECRET_KEY=minio_secret_key" > .env

# 2. Build and start the containers
docker-compose up -d --build
