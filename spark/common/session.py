"""SparkSession padronizado (Iceberg + Glue + S3A + AQE)."""

from __future__ import annotations

import os
import urllib.request
from typing import Optional

from pyspark.sql import SparkSession


def _ec2_private_ipv4() -> Optional[str]:
    meta = "http://169.254.169.254"
    try:
        req = urllib.request.Request(
            f"{meta}/latest/api/token",
            data=b"",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
        )
        with urllib.request.urlopen(req, timeout=3) as r:
            token = r.read().decode().strip()
        req2 = urllib.request.Request(
            f"{meta}/latest/meta-data/local-ipv4",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(req2, timeout=3) as r2:
            return r2.read().decode().strip() or None
    except Exception:
        try:
            with urllib.request.urlopen(f"{meta}/latest/meta-data/local-ipv4", timeout=2) as r:
                return r.read().decode().strip() or None
        except Exception:
            return None


def _master_host_from_spark_env_file() -> Optional[str]:
    path = "/opt/spark/conf/spark-env.sh"
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line.startswith("export SPARK_MASTER_HOST="):
                    _, _, val = line.partition("=")
                    return val.strip().strip('"').strip("'") or None
    except OSError:
        return None
    return None


def spark_master_url() -> str:
    env = os.environ.get("SPARK_MASTER_URL", "").strip()
    if env:
        return env
    ip = _ec2_private_ipv4()
    if not ip:
        ip = _master_host_from_spark_env_file()
    if not ip:
        raise RuntimeError(
            "Defina SPARK_MASTER_URL ou execute na EC2 Spark com SPARK_MASTER_HOST em spark-env.sh"
        )
    return f"spark://{ip}:7077"


def create_spark_session(app_name: str) -> SparkSession:
    """Cria SparkSession com catálogo Iceberg/Glue e tunings S3A."""
    master = spark_master_url()
    bucket = os.environ.get("DATALAKE_BUCKET", "austa-lakehouse-prod-data-lake-169446931765")
    region = os.environ.get("AWS_DEFAULT_REGION", "sa-east-1")

    driver_mem = os.environ.get("SPARK_DRIVER_MEMORY", "8g")
    executor_mem = os.environ.get("SPARK_EXECUTOR_MEMORY", "4g")
    executor_cores = os.environ.get("SPARK_EXECUTOR_CORES", "2")
    cores_max = os.environ.get("SPARK_CORES_MAX", "8")

    local_dir = os.environ.get("SPARK_LOCAL_DIR", "/var/lib/spark/local")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.submit.deployMode", "client")
        .config("spark.local.dir", local_dir)
        .config("spark.executor.local.dir", local_dir)
        .config(
            "spark.executor.extraJavaOptions",
            f"-Djava.io.tmpdir={local_dir} -XX:MetaspaceSize=256m",
        )
        .config(
            "spark.driver.extraJavaOptions",
            f"-Djava.io.tmpdir={local_dir} -XX:MetaspaceSize=256m",
        )
        .config("spark.driver.memory", driver_mem)
        .config("spark.executor.memory", executor_mem)
        .config("spark.executor.cores", executor_cores)
        .config("spark.cores.max", cores_max)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3a://{bucket}/")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.client.region", region)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.default.parallelism", os.environ.get("SPARK_DEFAULT_PARALLELISM", "8"))
        .config("spark.hadoop.fs.s3a.connection.maximum", "96")
        .config("spark.hadoop.fs.s3a.threads.max", "96")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
        .config("spark.sql.files.maxPartitionBytes", "268435456")
        .config("spark.sql.files.openCostInBytes", "8388608")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
