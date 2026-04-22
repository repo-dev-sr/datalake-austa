{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_prescricao', 'nr_seq_proc', 'cd_material', 'cd_unidade_medida',
      'ie_agrupador', 'ie_suspenso', 'ie_urgencia',
      'qt_material', 'qt_dispensada', 'qt_devolvida',
      'dt_prev_execucao', 'dt_execucao', 'dt_suspensao',
      'ds_observacao',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_prescr_proc_material', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_prescr_proc_material') }} b
  {% if is_incremental() %}
  WHERE b._bronze_loaded_at > (
      SELECT COALESCE(MAX(s._silver_processed_at), CAST('1900-01-01' AS TIMESTAMP))
      FROM {{ this }} s
  )
  {% endif %}
)

, latest_by_pk AS (
  SELECT *
  FROM (
    SELECT
        b.*
      , ROW_NUMBER() OVER (
          PARTITION BY b.nr_sequencia
          ORDER BY
              CAST(COALESCE(b._cdc_ts_ms, 0) AS BIGINT) DESC
            , CAST(COALESCE(b.__source_txid, 0) AS BIGINT) DESC
        ) AS _rn
    FROM base b
  ) x
  WHERE _rn = 1
)

, shaped AS (
  SELECT
      d.nr_sequencia                                              AS nr_sequencia
    , {{ fill_null_bigint('d.nr_prescricao', -1) }}               AS nr_prescricao
    , {{ fill_null_bigint('d.nr_seq_proc', -1) }}                 AS nr_seq_proc
    , {{ fill_null_bigint('d.cd_material', -1) }}                 AS cd_material
    , {{ fill_null_bigint('d.cd_unidade_medida', -1) }}           AS cd_unidade_medida

    , {{ standardize_enum('d.ie_agrupador', "'i'") }}             AS ie_agrupador
    , {{ standardize_enum('d.ie_suspenso', "'N'") }}              AS ie_suspenso
    , {{ standardize_enum('d.ie_urgencia', "'N'") }}              AS ie_urgencia

    , {{ normalize_decimal('d.qt_material', 4, -1) }}             AS qt_material
    , {{ normalize_decimal('d.qt_dispensada', 4, -1) }}           AS qt_dispensada
    , {{ normalize_decimal('d.qt_devolvida', 4, -1) }}            AS qt_devolvida

    , {{ standardize_date('d.dh_prev_execucao') }}                AS dt_prev_execucao
    , {{ standardize_date('d.dh_execucao') }}                     AS dt_execucao
    , {{ standardize_date('d.dh_suspensao') }}                    AS dt_suspensao

    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}      AS ds_observacao

    , {{ standardize_date('d.dh_atualizacao') }}                  AS dt_atualizacao
    , {{ standardize_text_initcap('d.nm_usuario', "'indefinido'") }}         AS nm_usuario
    , {{ standardize_date('d.dh_atualizacao_nrec') }}             AS dt_atualizacao_nrec
    , {{ standardize_text_initcap('d.nm_usuario_nrec', "'indefinido'") }}    AS nm_usuario_nrec

    {{ silver_audit_columns() }}
  FROM latest_by_pk d
)

, final AS (
  SELECT *
  FROM shaped
  WHERE NOT _is_deleted
)

SELECT * FROM final
