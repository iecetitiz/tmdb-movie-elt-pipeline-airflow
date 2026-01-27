from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# Configuration for the remote Spark Client Container
REMOTE_SCRIPT_DIR = "/dataops"
INGESTION_SCRIPT = "data_ingestion.py"
TRANSFORM_SCRIPT = "transform_delta_minio.py"
VENV_PYTHON_PATH = "/dataops/airflowenv/bin/python3" # Ensure this points to python3 in your venv
CHUNK_SIZE = 1000

# Ingestion Arguments
BUCKET_NAME = "tmdb-bronze"
FILE_KEY_MOVIES = "movies/movie"
FILE_KEY_CREDITS = "credits/credit"
GITHUB_URL_MOVIES = "https://raw.githubusercontent.com/harshitcodes/tmdb_movie_data_analysis/refs/heads/master/tmdb-5000-movie-dataset/tmdb_5000_movies.csv"
GITHUB_URL_CREDITS = "https://raw.githubusercontent.com/harshitcodes/tmdb_movie_data_analysis/refs/heads/master/tmdb-5000-movie-dataset/tmdb_5000_credits.csv"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'tmdb_etl_pipeline',
    default_args=default_args,
    description='TMDB ETL Pipeline: Ingestion and Silver Transformation via SSH',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Ingest Movie data from GitHub to MinIO Bronze Layer
    command_ingest_movies = f"""
        cd {REMOTE_SCRIPT_DIR} && \
        {VENV_PYTHON_PATH} {INGESTION_SCRIPT} \
        --bucket {BUCKET_NAME} \
        --key {FILE_KEY_MOVIES} \
        --input_file {GITHUB_URL_MOVIES} \
        --chunk_size {CHUNK_SIZE}
    """

    # Task 2: Ingest Credits data from GitHub to MinIO Bronze Layer
    command_ingest_credits = f"""
        cd {REMOTE_SCRIPT_DIR} && \
        {VENV_PYTHON_PATH} {INGESTION_SCRIPT} \
        --bucket {BUCKET_NAME} \
        --key {FILE_KEY_CREDITS} \
        --input_file {GITHUB_URL_CREDITS} \
        --chunk_size {CHUNK_SIZE}
    """

    # Task 3: Transform data from Bronze to Silver Layer using Spark-Submit
    # We export PYSPARK_PYTHON so Spark uses the venv containing 'python-dotenv'
    command_transform = f"""
        export PYSPARK_PYTHON={VENV_PYTHON_PATH} && \
        cd {REMOTE_SCRIPT_DIR} && \
        /opt/spark/bin/spark-submit \
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        {TRANSFORM_SCRIPT}
    """

    movies_ingestion = SSHOperator(
        task_id='movies_ingestion',
        ssh_conn_id='spark_ssh_conn',
        command=command_ingest_movies,
        conn_timeout=300,
        cmd_timeout=600
    )

    credits_ingestion = SSHOperator(
        task_id='credits_ingestion',
        ssh_conn_id='spark_ssh_conn',
        command=command_ingest_credits,
        conn_timeout=300,
        cmd_timeout=600
    )

    transform_data = SSHOperator(
        task_id="transform_data",
        ssh_conn_id='spark_ssh_conn',
        command=command_transform,
        conn_timeout=300, 
        cmd_timeout=600
    )

    # Pipeline Workflow: Sequential execution
    movies_ingestion >> credits_ingestion >> transform_data