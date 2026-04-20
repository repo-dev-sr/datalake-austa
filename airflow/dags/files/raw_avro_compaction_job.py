"""
Job utilitario para:
- reportar ultimo arquivo AVRO por topico (ordenado por recencia);
- compactar AVROs da ultima hora em arquivos alvo de ~128 MB por parte.

Modo --in-place: grava staging fora da pasta da hora, apaga AVROs originais na hora,
copia os compactados para o mesmo prefixo (o que a camada bronze le em raw_path).
"""
from __future__ import annotations

import argparse
import json
import math
import re
import uuid
from datetime import datetime, timedelta, timezone


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ranking e compactacao de AVROs raw")
    parser.add_argument("--mode", choices=["report", "compact"], required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--input-prefix", required=True)
    parser.add_argument(
        "--output-prefix",
        help="Destino separado (modo legado). Omitir se usar --in-place.",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Reescreve AVROs na mesma pasta da hora (staging sob __compaction_tmp no topico).",
    )
    parser.add_argument(
        "--target-size-mb",
        type=int,
        default=128,
        help="Tamanho alvo (MB) por ficheiro Avro de saida; a ultima parte pode ser menor.",
    )
    parser.add_argument("--execution-ts")
    parser.add_argument(
        "--hour-lag",
        type=int,
        default=0,
        help="Horas a recuar a partir da hora UTC cheia do execution-ts (0=mesma hora cheia; 1=hora anterior).",
    )
    parser.add_argument(
        "--partition-ts-override",
        default="",
        help="ISO UTC (ex. 2026-04-20T22:00:00+00:00) — usa esta hora de particao em vez de execution-ts + hour-lag.",
    )
    parser.add_argument(
        "--only-topic",
        default="",
        help="Substring do prefixo do topico (ex. AUSTA_CONTA ou austa.TASY.AUSTA_CONTA); restringe a compactacao.",
    )
    parser.add_argument(
        "--partition-from-latest-avro",
        action="store_true",
        help=(
            "Para cada topico, deriva a hora de particao a partir do ficheiro .avro mais recente no S3 "
            "(alinha com dados reais; evita usar --execution-ts de um dia sem dados nessa hora)."
        ),
    )
    parser.add_argument("--region", default="sa-east-1")
    return parser.parse_args()


_STREAM_BASES_KNOWN = (
    "raw/raw-tasy/stream/",
    "raw/raw_tasy/stream/",
)


def topic_slug(topic_prefix: str) -> str:
    for base in _STREAM_BASES_KNOWN:
        if topic_prefix.startswith(base):
            return topic_prefix[len(base) :].rstrip("/")
    parts = topic_prefix.rstrip("/").split("/")
    return parts[-1] if parts else topic_prefix


def list_topic_prefixes_merged(s3_client, bucket: str, primary_prefix: str) -> list[str]:
    """Lista topicos sob o prefixo principal e, se vazio, tenta variante raw-tasy/raw_tasy."""
    bases = [primary_prefix]
    for b in _STREAM_BASES_KNOWN:
        if b != primary_prefix and b not in bases:
            bases.append(b)
    merged: list[str] = []
    seen_slugs: set[str] = set()
    for base in bases:
        for tp in list_topic_prefixes(s3_client, bucket, base):
            slug = topic_slug(tp)
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            merged.append(tp)
    return merged


def list_topic_prefixes(s3_client, bucket: str, base_prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    topics: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            topics.append(common_prefix["Prefix"])
    return topics


def list_objects(s3_client, bucket: str, prefix: str) -> list[dict]:
    paginator = s3_client.get_paginator("list_objects_v2")
    objects: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def _s3_delete_keys(s3_client, bucket: str, keys: list[str]) -> None:
    batch: list[dict[str, str]] = []
    for key in keys:
        batch.append({"Key": key})
        if len(batch) >= 1000:
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            batch = []
    if batch:
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})


def _s3_list_keys_with_prefix(s3_client, bucket: str, prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _s3_delete_prefix(s3_client, bucket: str, prefix: str) -> None:
    keys = _s3_list_keys_with_prefix(s3_client, bucket, prefix)
    _s3_delete_keys(s3_client, bucket, keys)


def build_latest_by_topic_report(s3_client, bucket: str, input_prefix: str) -> list[dict]:
    topics = list_topic_prefixes(s3_client, bucket, input_prefix)
    rows: list[dict] = []

    for topic_prefix in topics:
        latest = None
        for obj in list_objects(s3_client, bucket, topic_prefix):
            key = obj["Key"]
            if not key.endswith(".avro"):
                continue
            if "__compaction_tmp" in key:
                continue
            if latest is None or obj["LastModified"] > latest["LastModified"]:
                latest = obj

        if latest is None:
            continue

        rows.append(
            {
                "topic": topic_prefix.removeprefix(input_prefix).rstrip("/"),
                "key": latest["Key"],
                "last_modified": latest["LastModified"].isoformat(),
                "size_bytes": latest["Size"],
            }
        )

    rows.sort(key=lambda item: item["last_modified"], reverse=True)
    return rows


def report_mode(args: argparse.Namespace) -> None:
    import boto3

    s3_client = boto3.client("s3", region_name=args.region)
    rows = build_latest_by_topic_report(s3_client, args.bucket, args.input_prefix)
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def topic_hour_prefix_candidates(topic_prefix: str, ts_utc: datetime) -> list[str]:
    year = ts_utc.strftime("%Y")
    month = ts_utc.strftime("%m")
    day = ts_utc.strftime("%d")
    hour = ts_utc.strftime("%H")
    return [
        f"{topic_prefix}year={year}/month={month}/day={day}/hour={hour}/",
        f"{topic_prefix}{year}/{month}/{day}/{hour}/",
    ]


def parse_partition_ts_from_s3_key(key: str) -> datetime | None:
    """Extrai o inicio UTC da pasta horaria a partir da key (Hive ou plano YYYY/MM/DD/HH)."""
    m = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/", key)
    if m:
        y, mo, d, h = m.groups()
        return datetime(int(y), int(mo), int(d), int(h), 0, 0, tzinfo=timezone.utc)
    m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/(\d{2})/", key)
    if m:
        y, mo, d, h = m.groups()
        return datetime(int(y), int(mo), int(d), int(h), 0, 0, tzinfo=timezone.utc)
    return None


def latest_partition_ts_from_topic(s3_client, bucket: str, topic_prefix: str) -> datetime | None:
    """Hora de particao (UTC) do .avro mais recente sob o prefixo do topico."""
    latest_key: str | None = None
    latest_mod = None
    for obj in list_objects(s3_client, bucket, topic_prefix):
        k = obj["Key"]
        if not k.endswith(".avro") or "__compaction_tmp" in k:
            continue
        if latest_mod is None or obj["LastModified"] > latest_mod:
            latest_mod = obj["LastModified"]
            latest_key = k
    if not latest_key:
        return None
    return parse_partition_ts_from_s3_key(latest_key)


def compact_mode(args: argparse.Namespace) -> None:
    override = (args.partition_ts_override or "").strip()
    exec_ts = (args.execution_ts or "").strip()
    partition_from_latest = args.partition_from_latest_avro

    if partition_from_latest and (override or exec_ts):
        raise ValueError(
            "Use --partition-from-latest-avro sozinho (sem --execution-ts nem --partition-ts-override)."
        )
    if not partition_from_latest and not exec_ts and not override:
        raise ValueError(
            "Em --mode compact informe --execution-ts, --partition-ts-override ou --partition-from-latest-avro"
        )
    if args.in_place and args.output_prefix:
        raise ValueError("Use apenas --in-place ou --output-prefix, nao ambos")
    if not args.in_place and not args.output_prefix:
        raise ValueError("Em --mode compact informe --in-place ou --output-prefix")

    import boto3

    from pyspark.sql import SparkSession

    fixed_partition_ts: datetime | None = None
    execution_label: str

    if partition_from_latest:
        execution_label = "partition-from-latest-avro"
    elif override:
        fixed_partition_ts = datetime.fromisoformat(override.replace("Z", "+00:00")).astimezone(timezone.utc)
        execution_label = override
    else:
        execution_ts_utc = datetime.fromisoformat(exec_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
        floored = execution_ts_utc.replace(minute=0, second=0, microsecond=0)
        fixed_partition_ts = floored - timedelta(hours=args.hour_lag)
        execution_label = exec_ts

    s3_client = boto3.client("s3", region_name=args.region)
    topics = list_topic_prefixes_merged(s3_client, args.bucket, args.input_prefix)
    only = (args.only_topic or "").strip()
    if only:
        topics = [t for t in topics if only in t]

    print(
        json.dumps(
            {
                "compact_meta": True,
                "partition_policy": (
                    "latest_avro_per_topic" if partition_from_latest else "fixed_execution_ts_or_override"
                ),
                "execution_ts": execution_label,
                "partition_ts_utc": (
                    fixed_partition_ts.isoformat() if fixed_partition_ts is not None else None
                ),
                "hour_lag": args.hour_lag,
                "partition_ts_override": bool(override),
                "only_topic": only or None,
                "input_prefix": args.input_prefix,
                "topic_prefix_count": len(topics),
            },
            ensure_ascii=False,
        )
    )

    target_bytes = args.target_size_mb * 1024 * 1024

    # SPARK-31404 / INCONSISTENT_BEHAVIOR_CROSS_VERSION.READ_ANCIENT_DATETIME: Avro CDC (Debezium)
    # pode conter date/timestamp interpretados no calendario "legacy"; sem LEGACY o Spark 3 falha ao ler.
    spark = (
        SparkSession.builder.appName("raw-avro-compactor-last-hour")
        .config("spark.sql.avro.compression.codec", "deflate")
        .config("spark.sql.avro.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.avro.dateRebaseModeInRead", "LEGACY")
        .config("spark.sql.avro.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.avro.dateRebaseModeInWrite", "LEGACY")
        .getOrCreate()
    )

    compacted_runs = 0
    skipped_topics = 0

    for topic_prefix in topics:
        if partition_from_latest:
            partition_ts = latest_partition_ts_from_topic(s3_client, args.bucket, topic_prefix)
            if partition_ts is None:
                skipped_topics += 1
                print(
                    json.dumps(
                        {
                            "skip_topic": topic_slug(topic_prefix),
                            "reason": "no_avro_or_unparseable_partition_from_latest_key",
                        },
                        ensure_ascii=False,
                    )
                )
                continue
        else:
            assert fixed_partition_ts is not None
            partition_ts = fixed_partition_ts

        selected_input_prefix = None
        input_objects: list[dict] = []
        for candidate in topic_hour_prefix_candidates(topic_prefix, partition_ts):
            objs = [
                obj
                for obj in list_objects(s3_client, args.bucket, candidate)
                if obj["Key"].endswith(".avro") and "__compaction_tmp" not in obj["Key"]
            ]
            if objs:
                selected_input_prefix = candidate
                input_objects = objs
                break

        if not selected_input_prefix:
            skipped_topics += 1
            print(
                json.dumps(
                    {
                        "skip_topic": topic_slug(topic_prefix),
                        "reason": "no_avro_in_partition_hour",
                        "partition_ts": partition_ts.isoformat(),
                        "candidates": topic_hour_prefix_candidates(topic_prefix, partition_ts),
                    },
                    ensure_ascii=False,
                )
            )
            continue

        total_bytes = sum(obj["Size"] for obj in input_objects)
        if total_bytes <= 0:
            skipped_topics += 1
            continue

        num_partitions = max(1, math.ceil(total_bytes / target_bytes))
        topic_name = topic_slug(topic_prefix)
        year = partition_ts.strftime("%Y")
        month = partition_ts.strftime("%m")
        day = partition_ts.strftime("%d")
        hour = partition_ts.strftime("%H")

        input_path = f"s3a://{args.bucket}/{selected_input_prefix}"
        original_keys = [obj["Key"] for obj in input_objects]

        if args.in_place:
            run_id = uuid.uuid4().hex
            staging_prefix = (
                f"{topic_prefix}__compaction_tmp/year={year}/month={month}/day={day}/hour={hour}/run-{run_id}/"
            )
            output_path = f"s3a://{args.bucket}/{staging_prefix}"
            log_payload = {
                "topic": topic_name,
                "mode": "in_place",
                "partition_ts_utc": partition_ts.isoformat(),
                "input_path": input_path,
                "staging_path": output_path,
                "total_input_bytes": total_bytes,
                "target_size_mb": args.target_size_mb,
                "num_partitions": num_partitions,
            }
        else:
            assert args.output_prefix is not None
            output_prefix = f"{args.output_prefix}{topic_name}/year={year}/month={month}/day={day}/hour={hour}/"
            output_path = f"s3a://{args.bucket}/{output_prefix}"
            log_payload = {
                "topic": topic_name,
                "mode": "separate_prefix",
                "partition_ts_utc": partition_ts.isoformat(),
                "input_path": input_path,
                "output_path": output_path,
                "total_input_bytes": total_bytes,
                "target_size_mb": args.target_size_mb,
                "num_partitions": num_partitions,
            }

        print(json.dumps(log_payload, ensure_ascii=False))

        df = spark.read.format("avro").load(input_path)
        (
            df.repartition(num_partitions)
            .write.mode("overwrite")
            .format("avro")
            .option("compression", "deflate")
            .save(output_path)
        )

        if args.in_place:
            staging_keys = [
                k
                for k in _s3_list_keys_with_prefix(s3_client, args.bucket, staging_prefix)
                if k.endswith(".avro")
            ]
            if not staging_keys:
                raise RuntimeError(
                    "Compactacao abortada: nenhum .avro listado no staging apos write Spark; "
                    f"originais nao foram apagados. staging_prefix={staging_prefix}"
                )
            _s3_delete_keys(s3_client, args.bucket, original_keys)
            for src_key in staging_keys:
                base = src_key.rsplit("/", maxsplit=1)[-1]
                dst_key = f"{selected_input_prefix}{base}"
                s3_client.copy_object(
                    Bucket=args.bucket,
                    Key=dst_key,
                    CopySource={"Bucket": args.bucket, "Key": src_key},
                )
            _s3_delete_prefix(s3_client, args.bucket, staging_prefix)

        compacted_runs += 1

    print(
        json.dumps(
            {
                "compact_summary": True,
                "topic_prefix_count": len(topics),
                "compacted_topic_hours": compacted_runs,
                "skipped_topics_no_data": skipped_topics,
            },
            ensure_ascii=False,
        )
    )

    spark.stop()


def main() -> None:
    args = parse_args()
    if args.mode == "report":
        report_mode(args)
    else:
        compact_mode(args)


if __name__ == "__main__":
    main()

