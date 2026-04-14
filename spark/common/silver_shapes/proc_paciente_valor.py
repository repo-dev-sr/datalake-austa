"""Shape nativo PySpark — silver_tasy_proc_paciente_valor."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from common.macros import (
    add_silver_audit_columns,
    fill_null_bigint,
    normalize_decimal,
    rename_audit_for_silver,
    standardize_date,
    standardize_enum,
    standardize_text_initcap,
)


def shape_silver_proc_paciente_valor(d: DataFrame) -> DataFrame:
    out = d.select(
        fill_null_bigint("nr_seq_procedimento"),
        f.col("nr_sequencia"),
        standardize_enum("ie_tipo_valor").alias("ie_tipo_valor"),
        standardize_date("dh_atualizacao").alias("dt_atualizacao"),
        standardize_text_initcap("nm_usuario").alias("nm_usuario"),
        normalize_decimal("vl_procedimento", 2, -1.0).alias("vl_procedimento"),
        normalize_decimal("vl_medico", 2, -1.0).alias("vl_medico"),
        normalize_decimal("vl_anestesista", 2, -1.0).alias("vl_anestesista"),
        normalize_decimal("vl_materiais", 2, -1.0).alias("vl_materiais"),
        normalize_decimal("vl_auxiliares", 2, -1.0).alias("vl_auxiliares"),
        normalize_decimal("vl_custo_operacional", 2, -1.0).alias("vl_custo_operacional"),
        fill_null_bigint("cd_convenio"),
        fill_null_bigint("cd_categoria"),
        normalize_decimal("pr_valor", 2, -1.0).alias("pr_valor"),
        fill_null_bigint("nr_seq_trans_fin"),
        fill_null_bigint("nr_lote_contabil"),
        fill_null_bigint("nr_seq_desconto"),
        normalize_decimal("qt_pontos", 2, -1.0).alias("qt_pontos"),
        fill_null_bigint("nr_codigo_controle"),
        fill_null_bigint("nr_seq_partic"),
        f.col("_is_deleted"),
        f.col("_cdc_op"),
        f.col("_cdc_event_at"),
        f.col("_cdc_ts_ms"),
        f.col("_row_hash"),
        f.col("_bronze_loaded_at"),
    )
    out = rename_audit_for_silver(out)
    return add_silver_audit_columns(out)
