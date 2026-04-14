"""
Compactação de Avros small-files no S3 raw.
Escreve arquivos compactados no disco local EBS, e depois usa aws s3 sync para subir.
"""
from __future__ import annotations
import os
import subprocess
import sys
import time

from pyspark.sql import SparkSession

ENTITY = "tasy.TASY.PROCEDIMENTO_PACIENTE"
BUCKET = "austa-lakehouse-prod-data-lake-169446931765"
RAW_PATH = f"s3a://{BUCKET}/raw/raw-tasy/stream/{ENTITY}/"
ORIGINAL_S3 = f"s3://{BUCKET}/raw/raw-tasy/stream/{ENTITY}/"
BACKUP_S3 = f"s3://{BUCKET}/raw/raw-tasy/_backup_pre_compact/{ENTITY}/"
LOCAL_DIR = "/var/lib/spark/compact_avro_out"
TARGET_FILE_COUNT = 50

os.environ["PYTHONUNBUFFERED"] = "1"

print("=" * 60, flush=True)
print("COMPACTACAO AVRO - raw PROCEDIMENTO_PACIENTE", flush=True)
print(f"Inicio: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
print(f"Source:  {RAW_PATH}", flush=True)
print(f"Local:   {LOCAL_DIR}", flush=True)
print(f"Target:  {TARGET_FILE_COUNT} arquivos", flush=True)
print("=" * 60, flush=True)

t0 = time.time()

spark = (
    SparkSession.builder
    .appName("compact_avro_raw_procedimento_paciente")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.files.openCostInBytes", "1048576")
    .config("spark.sql.files.maxPartitionBytes", "268435456")
    .config("spark.hadoop.fs.s3a.connection.maximum", "200")
    .config("spark.hadoop.fs.s3a.threads.max", "200")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 1. Ler todos os Avros ─────────────────────────────────────────
print("\n[1/5] Lendo todos os Avros raw...", flush=True)
t1 = time.time()
df = spark.read.format("avro").load(RAW_PATH)
row_count = df.count()
t_read = time.time() - t1
print(f"  Linhas: {row_count:,}", flush=True)
print(f"  Tempo:  {t_read:.1f}s", flush=True)

# ── 2. Escrever compactados no disco local ─────────────────────────
print(f"\n[2/5] Escrevendo {TARGET_FILE_COUNT} Avros no disco local {LOCAL_DIR}...", flush=True)
t2 = time.time()
os.makedirs(LOCAL_DIR, exist_ok=True)
df.coalesce(TARGET_FILE_COUNT).write.format("avro").mode("overwrite").save(f"file://{LOCAL_DIR}")
t_write = time.time() - t2
print(f"  Escrita local: {t_write:.1f}s", flush=True)

local_files = [f for f in os.listdir(LOCAL_DIR) if f.endswith(".avro")]
print(f"  Arquivos gerados: {len(local_files)}", flush=True)

spark.stop()

# ── 3. Backup dos originais no S3 ─────────────────────────────────
print(f"\n[3/5] Backup: {ORIGINAL_S3} -> {BACKUP_S3}", flush=True)
t3 = time.time()
r = subprocess.run(
    ["aws", "s3", "sync", ORIGINAL_S3, BACKUP_S3, "--quiet"],
    capture_output=True, text=True, timeout=600,
)
t_backup = time.time() - t3
if r.returncode != 0:
    print(f"  ERRO backup: {r.stderr}", flush=True)
    sys.exit(1)
print(f"  Backup em {t_backup:.1f}s", flush=True)

# ── 4. Deletar originais no S3 ────────────────────────────────────
print(f"\n[4/5] Deletando originais: {ORIGINAL_S3}", flush=True)
t4 = time.time()
r = subprocess.run(
    ["aws", "s3", "rm", ORIGINAL_S3, "--recursive", "--quiet"],
    capture_output=True, text=True, timeout=600,
)
t_del = time.time() - t4
if r.returncode != 0:
    print(f"  ERRO delete: {r.stderr}", flush=True)
    sys.exit(1)
print(f"  Deletados em {t_del:.1f}s", flush=True)

# ── 5. Upload compactados para S3 ─────────────────────────────────
print(f"\n[5/5] Upload: {LOCAL_DIR} -> {ORIGINAL_S3}", flush=True)
t5 = time.time()
r = subprocess.run(
    ["aws", "s3", "sync", LOCAL_DIR, ORIGINAL_S3,
     "--exclude", "*", "--include", "*.avro", "--quiet"],
    capture_output=True, text=True, timeout=600,
)
t_up = time.time() - t5
if r.returncode != 0:
    print(f"  ERRO upload: {r.stderr}", flush=True)
    sys.exit(1)
print(f"  Upload em {t_up:.1f}s", flush=True)

# Cleanup local
subprocess.run(["rm", "-rf", LOCAL_DIR], check=False)

t_total = time.time() - t0
print("\n" + "=" * 60, flush=True)
print("COMPACTACAO CONCLUIDA", flush=True)
print(f"  Linhas:         {row_count:,}", flush=True)
print(f"  Arquivos:       ~4957 -> {len(local_files)}", flush=True)
print(f"  Tempo leitura:  {t_read:.1f}s", flush=True)
print(f"  Tempo escrita:  {t_write:.1f}s", flush=True)
print(f"  Tempo backup:   {t_backup:.1f}s", flush=True)
print(f"  Tempo delete:   {t_del:.1f}s", flush=True)
print(f"  Tempo upload:   {t_up:.1f}s", flush=True)
print(f"  TOTAL:          {t_total:.1f}s", flush=True)
print("=" * 60, flush=True)
