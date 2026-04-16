"""
Job utilitario para:
- reportar ultimo arquivo AVRO por topico (ordenado por recencia);
- compactar AVROs da ultima hora em arquivos alvo de ~100 MB.

Modo --in-place: grava staging fora da pasta da hora, apaga AVROs originais na hora,
copia os compactados para o mesmo prefixo (o que a camada bronze le em raw_path).
"""
from __future__ import annotations

import argparse
import json
import math
import uuid
from datetime import datetime, timedelta, timezone

import boto3


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
    parser.add_argument("--target-size-mb", type=int, default=100)
    parser.add_argument("--execution-ts")
    parser.add_argument("--region", default="sa-east-1")
    return parser.parse_args()


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


def compact_mode(args: argparse.Namespace) -> None:
    if not args.execution_ts:
        raise ValueError("--execution-ts e obrigatorio em --mode compact")
    if args.in_place and args.output_prefix:
        raise ValueError("Use apenas --in-place ou --output-prefix, nao ambos")
    if not args.in_place and not args.output_prefix:
        raise ValueError("Em --mode compact informe --in-place ou --output-prefix")

    from pyspark.sql import SparkSession

    execution_ts_utc = datetime.fromisoformat(args.execution_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    hour_ts_utc = execution_ts_utc - timedelta(hours=1)

    s3_client = boto3.client("s3", region_name=args.region)
    topics = list_topic_prefixes(s3_client, args.bucket, args.input_prefix)

    target_bytes = args.target_size_mb * 1024 * 1024

    spark = (
        SparkSession.builder.appName("raw-avro-compactor-last-hour")
        .config("spark.sql.avro.compression.codec", "deflate")
        .getOrCreate()
    )

    for topic_prefix in topics:
        selected_input_prefix = None
        input_objects: list[dict] = []
        for candidate in topic_hour_prefix_candidates(topic_prefix, hour_ts_utc):
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
            continue

        total_bytes = sum(obj["Size"] for obj in input_objects)
        if total_bytes <= 0:
            continue

        num_partitions = max(1, math.ceil(total_bytes / target_bytes))
        topic_name = topic_prefix.removeprefix(args.input_prefix).rstrip("/")
        year = hour_ts_utc.strftime("%Y")
        month = hour_ts_utc.strftime("%m")
        day = hour_ts_utc.strftime("%d")
        hour = hour_ts_utc.strftime("%H")

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

    spark.stop()


def main() -> None:
    args = parse_args()
    if args.mode == "report":
        report_mode(args)
    else:
        compact_mode(args)


if __name__ == "__main__":
    main()
