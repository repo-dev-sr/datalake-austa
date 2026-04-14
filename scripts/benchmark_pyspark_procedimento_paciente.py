from __future__ import annotations

import os
import sys
import time
import urllib.request
from typing import Optional

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
except ImportError:
    print("ERRO: PySpark nao encontrado. Execute via spark-submit.")
    sys.exit(1)

BRONZE_ONLY = "--bronze-only" in sys.argv


def _ec2_private_ipv4() -> Optional[str]:
    """IMDSv2 (obrigatorio em muitas contas AL2023); fallback IMDSv1."""
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
            ip = r2.read().decode().strip()
        return ip or None
    except Exception:
        try:
            with urllib.request.urlopen(f"{meta}/latest/meta-data/local-ipv4", timeout=2) as r:
                ip = r.read().decode().strip()
            return ip or None
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


def _spark_master_url() -> str:
    env = os.environ.get("SPARK_MASTER_URL", "").strip()
    if env:
        return env
    ip = _ec2_private_ipv4()
    if not ip:
        ip = _master_host_from_spark_env_file()
    if not ip:
        raise RuntimeError(
            "Nao foi possivel resolver IP do Spark master. "
            "Defina SPARK_MASTER_URL, ex.: export SPARK_MASTER_URL=spark://172.36.2.222:7077"
        )
    return f"spark://{ip}:7077"


print("=" * 60, flush=True)
print("BENCHMARK AUSTA - PySpark puro", flush=True)
print(f"Inicio: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
print(f"Modo: {'BRONZE apenas' if BRONZE_ONLY else 'Bronze + Silver + Silver Context'}", flush=True)
print("=" * 60, flush=True)

start_total = time.time()
master = _spark_master_url()
print(f"spark.master = {master}", flush=True)

builder = (
    SparkSession.builder.appName("benchmark_pyspark_procedimento_paciente")
    .master(master)
    .config("spark.submit.deployMode", "client")
    .config("spark.local.dir", "/var/lib/spark/local")
    .config("spark.executor.local.dir", "/var/lib/spark/local")
    .config(
        "spark.executor.extraJavaOptions",
        "-Djava.io.tmpdir=/var/lib/spark/local -XX:MetaspaceSize=256m",
    )
    .config(
        "spark.driver.extraJavaOptions",
        "-Djava.io.tmpdir=/var/lib/spark/local -XX:MetaspaceSize=256m",
    )
    .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
    .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
    .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "2"))
    .config("spark.cores.max", os.environ.get("SPARK_CORES_MAX", "4"))
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3a://austa-lakehouse-prod-data-lake-169446931765/")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.client.region", "sa-east-1")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .config("spark.hadoop.fs.s3a.connection.maximum", "96")
    .config("spark.hadoop.fs.s3a.threads.max", "96")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

RAW_PATH = (
    "s3a://austa-lakehouse-prod-data-lake-169446931765/"
    "raw/raw-tasy/stream/tasy.TASY.PROCEDIMENTO_PACIENTE/"
)
CATALOG = "glue_catalog"
BRONZE_TB = f"{CATALOG}.bronze.bronze_tasy_procedimento_paciente"
SILVER_TB = f"{CATALOG}.silver.silver_tasy_procedimento_paciente"
CONV_TB = f"{CATALOG}.silver.silver_tasy_proc_paciente_convenio"
ATEND_TB = f"{CATALOG}.silver.silver_tasy_atendimento_paciente"


def fill_null_bigint(col_name, default=-1):
    return F.when(F.col(col_name).isNull(), default).otherwise(F.col(col_name).cast("long"))


def normalize_decimal(col_name, scale=2, default=-1.0):
    return F.when(F.col(col_name).isNull(), default).otherwise(F.round(F.col(col_name).cast("double"), scale))


def standardize_date(col_name):
    return F.when(
        F.col(col_name).isNull(), F.to_timestamp(F.lit("1900-01-01"), "yyyy-MM-dd")
    ).otherwise(F.to_timestamp((F.col(col_name).cast("long") / 1000).cast("long").cast("timestamp")))


def standardize_text_initcap(col_name, default="'indefinido'"):
    default_val = default.strip("'")
    return F.when(F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""), F.lit(default_val)).otherwise(
        F.initcap(F.trim(F.col(col_name)))
    )


def standardize_enum(col_name, default="'i'"):
    default_val = default.strip("'")
    return F.when(F.col(col_name).isNull(), F.lit(default_val)).otherwise(F.lower(F.trim(F.col(col_name))))


print("\n--- [1/3] BRONZE ---", flush=True)
t0 = time.time()
try:
    wm_row = spark.sql(
        f"SELECT CAST(COALESCE(MAX(_cdc_ts_ms), 0) AS BIGINT) AS max_ts_ms FROM {BRONZE_TB}"
    ).collect()[0]
    max_ts_ms = wm_row["max_ts_ms"]
    wm_start_ms = max(0, max_ts_ms - (2 * 3600 * 1000))
    print(f"  watermark_start_ms={wm_start_ms}", flush=True)
except Exception as exc:
    wm_start_ms = 0
    print(f"  watermark full scan (bronze metadata: {exc})", flush=True)

_ts = F.col("__ts_ms").cast("long")
# Uma linha de leitura: filtro cedo + projecao unica (evita segundo scan em raw_df.count)
bronze_df = (
    spark.read.format("avro")
    .load(RAW_PATH)
    .where(F.coalesce(_ts, F.lit(0)) >= F.lit(wm_start_ms))
    .select(
        F.col("nr_sequencia"),
        F.col("nr_atendimento"),
        F.col("dt_entrada_unidade").alias("dh_entrada_unidade"),
        F.col("cd_procedimento"),
        F.col("dt_procedimento").alias("dh_procedimento"),
        F.col("qt_procedimento"),
        F.col("dt_atualizacao").alias("dh_atualizacao"),
        F.col("nm_usuario"),
        F.col("cd_medico"),
        F.col("cd_convenio"),
        F.col("cd_categoria"),
        F.col("cd_pessoa_fisica"),
        F.col("dt_prescricao").alias("dh_prescricao"),
        F.col("ds_observacao"),
        F.col("vl_procedimento"),
        F.col("vl_medico"),
        F.col("vl_anestesista"),
        F.col("vl_materiais"),
        F.col("cd_medico_executor"),
        F.col("cd_setor_atendimento"),
        F.col("cd_especialidade"),
        F.col("dt_inicio_procedimento").alias("dh_inicio_procedimento"),
        F.col("dt_final_procedimento").alias("dh_final_procedimento"),
        _ts.alias("_cdc_ts_ms"),
        F.col("__op").alias("_cdc_op"),
        F.to_timestamp((_ts / 1000).cast("long").cast("timestamp")).alias("_cdc_event_at"),
        F.current_timestamp().alias("_bronze_loaded_at"),
        F.md5(
            F.concat_ws(
                "|",
                F.col("nr_sequencia").cast("string"),
                F.col("nr_atendimento").cast("string"),
                F.col("cd_procedimento").cast("string"),
                F.col("dt_procedimento").cast("string"),
                F.col("vl_procedimento").cast("string"),
            )
        ).alias("_bronze_row_hash"),
        F.when(F.col("__op") == F.lit("d"), True).otherwise(False).alias("_is_deleted"),
        F.lit("pyspark_benchmark").alias("_dbt_invocation_id"),
        F.col("__source_txid"),
    )
)

# Uma acao materializa leitura Avro + projecao (sem segundo count em DF bruto)
count_bronze_rows = bronze_df.count()
t_bronze = time.time() - t0
print(f"  Linhas candidatas (bronze transform): {count_bronze_rows:,}", flush=True)
print(f"  Tempo Bronze (leitura + projecao + count): {t_bronze:.2f}s", flush=True)

if BRONZE_ONLY:
    t_total = time.time() - start_total
    print("\n" + "=" * 60, flush=True)
    print("RESULTADO - somente BRONZE", flush=True)
    print("=" * 60, flush=True)
    print(f"  Bronze: {t_bronze:.2f}s", flush=True)
    print(f"  TOTAL:  {t_total:.2f}s", flush=True)
    print("=" * 60, flush=True)
    spark.stop()
    sys.exit(0)

print("\n--- [2/3] SILVER ---")
t1 = time.time()
try:
    silver_wm = spark.sql(
        f"SELECT COALESCE(MAX(_silver_processed_at), CAST('1900-01-01' AS TIMESTAMP)) AS max_ts FROM {SILVER_TB}"
    ).collect()[0]["max_ts"]
    bronze_incremental = bronze_df.filter(F.col("_bronze_loaded_at") > silver_wm)
except Exception:
    bronze_incremental = bronze_df

window_dedup = Window.partitionBy("nr_sequencia").orderBy(
    F.coalesce(F.col("_cdc_ts_ms"), F.lit(0)).desc(),
    F.coalesce(F.col("__source_txid").cast("long"), F.lit(0)).desc(),
)
deduped_df = bronze_incremental.withColumn("_rn", F.row_number().over(window_dedup)).filter(F.col("_rn") == 1).drop(
    "_rn"
)

silver_df = (
    deduped_df.withColumn("nr_atendimento", fill_null_bigint("nr_atendimento"))
    .withColumn("cd_procedimento", fill_null_bigint("cd_procedimento"))
    .withColumn("cd_medico", fill_null_bigint("cd_medico"))
    .withColumn("cd_convenio", fill_null_bigint("cd_convenio"))
    .withColumn("cd_categoria", fill_null_bigint("cd_categoria"))
    .withColumn("cd_pessoa_fisica", fill_null_bigint("cd_pessoa_fisica"))
    .withColumn("cd_especialidade", fill_null_bigint("cd_especialidade"))
    .withColumn("cd_medico_executor", fill_null_bigint("cd_medico_executor"))
    .withColumn("cd_setor_atendimento", fill_null_bigint("cd_setor_atendimento"))
    .withColumn("vl_procedimento", normalize_decimal("vl_procedimento"))
    .withColumn("vl_medico", normalize_decimal("vl_medico"))
    .withColumn("vl_anestesista", normalize_decimal("vl_anestesista"))
    .withColumn("vl_materiais", normalize_decimal("vl_materiais"))
    .withColumn("qt_procedimento", normalize_decimal("qt_procedimento"))
    .withColumn("nm_usuario", standardize_text_initcap("nm_usuario"))
    .withColumn("ds_observacao", standardize_text_initcap("ds_observacao"))
    .withColumn("dh_entrada_unidade", standardize_date("dh_entrada_unidade"))
    .withColumn("dh_procedimento", standardize_date("dh_procedimento"))
    .withColumn("dh_atualizacao", standardize_date("dh_atualizacao"))
    .withColumn("dh_prescricao", standardize_date("dh_prescricao"))
    .withColumn("dh_inicio_procedimento", standardize_date("dh_inicio_procedimento"))
    .withColumn("dh_final_procedimento", standardize_date("dh_final_procedimento"))
    .withColumn("_silver_processed_at", F.current_timestamp())
    .filter(F.col("_is_deleted") == False)
)
count_silver = silver_df.count()
t_silver = time.time() - t1

print("\n--- [3/3] SILVER CONTEXT ---")
t2 = time.time()
try:
    conv_df = spark.table(CONV_TB)
    atend_df = spark.table(ATEND_TB)
    sc_df = (
        silver_df.alias("proc")
        .join(conv_df.alias("conv"), F.col("proc.nr_sequencia") == F.col("conv.nr_seq_procedimento"), "left")
        .join(atend_df.alias("atend"), F.col("proc.nr_atendimento") == F.col("atend.nr_atendimento"), "left")
        .select(
            F.col("proc.nr_sequencia"),
            F.col("proc.nr_atendimento"),
            F.col("proc.cd_procedimento"),
            F.col("proc.dh_procedimento").alias("dt_procedimento"),
            F.col("proc.qt_procedimento"),
            F.col("proc.vl_procedimento"),
            F.col("proc.vl_medico"),
            F.col("proc.vl_anestesista"),
            F.col("proc.vl_materiais"),
            F.col("proc.cd_medico"),
            F.col("proc.cd_convenio"),
            F.col("proc.cd_categoria"),
            F.col("proc.cd_pessoa_fisica"),
            F.col("proc.cd_especialidade"),
            F.col("proc.nm_usuario"),
            F.col("proc.dh_entrada_unidade").alias("dt_entrada_unidade"),
            F.col("proc.dh_inicio_procedimento").alias("dt_inicio_procedimento"),
            F.col("proc.dh_final_procedimento").alias("dt_final_procedimento"),
            standardize_text_initcap("conv.ds_procedimento").alias("ds_procedimento"),
            fill_null_bigint("conv.cd_unidade_medida").alias("cd_unidade_medida"),
            normalize_decimal("conv.tx_conversao_qtde", 4).alias("tx_conversao_qtde"),
            fill_null_bigint("conv.cd_grupo").alias("cd_grupo"),
            fill_null_bigint("conv.nr_proc_interno").alias("nr_proc_interno"),
            standardize_enum("atend.ie_tipo_atendimento").alias("atend_ie_tipo_atendimento"),
            standardize_date("atend.dt_entrada").alias("atend_dt_entrada"),
            standardize_date("atend.dt_alta").alias("atend_dt_alta"),
            standardize_enum("atend.ie_status_atendimento").alias("atend_ie_status_atendimento"),
            fill_null_bigint("atend.cd_medico_resp").alias("atend_cd_medico_resp"),
            standardize_enum("atend.ie_clinica").alias("atend_ie_clinica"),
            fill_null_bigint("atend.cd_estabelecimento").alias("atend_cd_estabelecimento"),
            fill_null_bigint("atend.cd_procedencia").alias("atend_cd_procedencia"),
            fill_null_bigint("atend.cd_motivo_alta").alias("atend_cd_motivo_alta"),
            F.current_timestamp().alias("_silver_context_processed_at"),
        )
    )
    count_sc = sc_df.count()
except Exception:
    count_sc = 0
t_sc = time.time() - t2

t_total = time.time() - start_total
print("\n" + "=" * 60)
print("RESULTADO FINAL - PySpark puro")
print("=" * 60)
print(f"  Bronze linhas:     {count_bronze_rows:,}")
print(f"  Silver linhas:     {count_silver:,}")
print(f"  Silver Context:    {count_sc:,}")
print(f"  Bronze:           {t_bronze:.2f}s")
print(f"  Silver:           {t_silver:.2f}s")
print(f"  Silver Context:   {t_sc:.2f}s")
print(f"  TOTAL:            {t_total:.2f}s")
print("=" * 60)

spark.stop()
