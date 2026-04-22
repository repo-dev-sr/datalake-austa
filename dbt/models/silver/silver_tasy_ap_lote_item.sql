{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_seq_lote', 'nr_seq_mat_hor', 'cd_material', 'cd_material_original',
      'cd_unidade_medida', 'cd_local_consistido', 'nr_prescricao', 'nr_seq_prescr_mat',
      'qt_dispensada', 'qt_estoque', 'qt_prescrita', 'qt_real_disp',
      'ie_prescrito', 'ie_urgente', 'ie_item_substituido', 'ie_controlado', 'ie_gerado',
      'ie_status_item',
      'dt_dispensacao', 'dt_geracao', 'dt_entrega', 'dt_recebimento', 'dt_validade',
      'ds_observacao', 'ds_justificativa', 'nr_lote_fabric',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_ap_lote_item', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_ap_lote_item') }} b
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
    , {{ fill_null_bigint('d.nr_seq_lote', -1) }}                 AS nr_seq_lote
    , {{ fill_null_bigint('d.nr_seq_mat_hor', -1) }}              AS nr_seq_mat_hor
    , {{ fill_null_bigint('d.cd_material', -1) }}                 AS cd_material
    , {{ fill_null_bigint('d.cd_material_original', -1) }}        AS cd_material_original
    , {{ fill_null_bigint('d.cd_unidade_medida', -1) }}           AS cd_unidade_medida
    , {{ fill_null_bigint('d.cd_local_consistido', -1) }}         AS cd_local_consistido
    , {{ fill_null_bigint('d.nr_prescricao', -1) }}               AS nr_prescricao
    , {{ fill_null_bigint('d.nr_seq_prescr_mat', -1) }}           AS nr_seq_prescr_mat

      -- Quantidades (outliers saneados: QT < 0 -> NULL, QT_TOTAL_DISPENSAR > 10000 -> NULL)
    , CASE
        WHEN d.qt_dispensada < 0 THEN NULL
        ELSE {{ normalize_decimal('d.qt_dispensada', 4, -1) }}
      END                                                         AS qt_dispensada
    , {{ normalize_decimal('d.qt_estoque', 4, -1) }}              AS qt_estoque
    , {{ normalize_decimal('d.qt_prescrita', 4, -1) }}            AS qt_prescrita
    , CASE
        WHEN d.qt_real_disp > 10000 THEN NULL
        ELSE {{ normalize_decimal('d.qt_real_disp', 4, -1) }}
      END                                                         AS qt_real_disp

      -- Categoricos
    , {{ standardize_enum('d.ie_prescrito', "'S'") }}             AS ie_prescrito
    , {{ standardize_enum('d.ie_urgente', "'N'") }}               AS ie_urgente
    , {{ standardize_enum('d.ie_item_substituido', "'N'") }}      AS ie_item_substituido
    , {{ standardize_enum('d.ie_controlado', "'N'") }}            AS ie_controlado
    , {{ standardize_enum('d.ie_gerado', "'N'") }}                AS ie_gerado
    , {{ standardize_enum('d.ie_status_item', "'i'") }}           AS ie_status_item

      -- Timestamps (dh_* -> dt_*)
    , {{ standardize_date('d.dh_dispensacao') }}                  AS dt_dispensacao
    , {{ standardize_date('d.dh_geracao') }}                      AS dt_geracao
    , {{ standardize_date('d.dh_entrega') }}                      AS dt_entrega
    , {{ standardize_date('d.dh_recebimento') }}                  AS dt_recebimento
    , {{ standardize_date('d.dh_validade') }}                     AS dt_validade

      -- Observacoes
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}      AS ds_observacao
    , {{ standardize_text_initcap('d.ds_justificativa', "'indefinido'") }}   AS ds_justificativa
    , {{ standardize_text_initcap('d.nr_lote_fabric', "'indefinido'") }}     AS nr_lote_fabric

      -- Auditoria TASY
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
