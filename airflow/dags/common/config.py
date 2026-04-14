"""
Configuration module for Airflow DAGs.
Centralizes environment variables and runtime configuration.
"""
import os

# Data Lake
DATALAKE_BUCKET = os.environ.get("DATALAKE_BUCKET", "austa-lakehouse-prod-data-lake-169446931765")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "sa-east-1")

# Spark / dbt-spark
SPARK_HOST = os.environ.get("SPARK_HOST", "177.71.255.159")
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://177.71.255.159:7077")
SPARK_THRIFT_HOST = os.environ.get("SPARK_THRIFT_HOST", "177.71.255.159")
SPARK_THRIFT_URL = os.environ.get("SPARK_THRIFT_URL", "jdbc:hive2://177.71.255.159:10000/raw")
SPARK_THRIFT_USER = os.environ.get("SPARK_THRIFT_USER", "")
SPARK_THRIFT_PASSWORD = os.environ.get("SPARK_THRIFT_PASSWORD", "")

# PySpark (spark-submit remoto na EC2 Spark — SSH a partir do worker Airflow)
SPARK_SSH_USER = os.environ.get("SPARK_SSH_USER", "ec2-user")
SPARK_SSH_KEY = os.environ.get("SPARK_SSH_KEY", "/opt/airflow/ssh/spark.pem")
SPARK_SCRIPTS_DIR = os.environ.get("SPARK_SCRIPTS_DIR", "/opt/spark/lakehouse")
SPARK_SUBMIT_BIN = os.environ.get("SPARK_SUBMIT_BIN", "/opt/spark/bin/spark-submit")

# dbt
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt")
# Chave do profile em profiles.yml (no servidor pode ser diferente do local; ex.: conn_id do deploy)
DBT_PROFILE_NAME = os.environ.get("DBT_PROFILE_NAME", "lakehouse_tasy")
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")
