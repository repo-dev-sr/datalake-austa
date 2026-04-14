"""Shape nativo PySpark — silver_tasy_proc_paciente_convenio (paridade SQL gerado)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from common.macros import (
    add_silver_audit_columns,
    fill_null_bigint,
    normalize_decimal,
    rename_audit_for_silver,
    standardize_date,
    standardize_text_initcap,
)


def shape_silver_proc_paciente_convenio(d: DataFrame) -> DataFrame:
    out = d.select(
        f.col("nr_seq_procedimento"),
        standardize_date("dh_atualizacao").alias("dt_atualizacao"),
        standardize_text_initcap("nm_usuario").alias("nm_usuario"),
        fill_null_bigint("cd_procedimento"),
        standardize_text_initcap("ds_procedimento").alias("ds_procedimento"),
        fill_null_bigint("cd_unidade_medida"),
        normalize_decimal("tx_conversao_qtde", 4, -1.0).alias("tx_conversao_qtde"),
        fill_null_bigint("cd_grupo"),
        fill_null_bigint("nr_proc_interno"),
        f.col("_is_deleted"),
        f.col("_cdc_op"),
        f.col("_cdc_event_at"),
        f.col("_cdc_ts_ms"),
        f.col("_row_hash"),
        f.col("_bronze_loaded_at"),
    )
    out = rename_audit_for_silver(out)
    return add_silver_audit_columns(out)
