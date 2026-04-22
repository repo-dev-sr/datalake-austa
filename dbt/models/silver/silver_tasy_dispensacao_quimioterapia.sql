{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{#- Dispensacao oncologica - fluxo proprio para quimioterapia -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_atendimento', 'nr_prescricao', 'cd_material', 'nr_seq_item',
      'cd_pessoa_fisica', 'cd_setor_atendimento',
      'qt_material', 'qt_dispensada', 'qt_devolvida',
      'ie_status', 'ie_tipo_dispensacao', 'ie_controlado',
      'dt_dispensacao', 'dt_preparacao', 'dt_cancelamento',
      'ds_observacao', 'ds_justificativa',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_dispensacao_quimioterapia', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_dispensacao_quimioterapia') }} b
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
    , {{ fill_null_bigint('d.nr_atendimento', -1) }}              AS nr_atendimento
    , {{ fill_null_bigint('d.nr_prescricao', -1) }}               AS nr_prescricao
    , {{ fill_null_bigint('d.cd_material', -1) }}                 AS cd_material
    , {{ fill_null_bigint('d.nr_seq_item', -1) }}                 AS nr_seq_item
    , {{ fill_null_bigint('d.cd_pessoa_fisica', -1) }}            AS cd_pessoa_fisica
    , {{ fill_null_bigint('d.cd_setor_atendimento', -1) }}        AS cd_setor_atendimento

    , {{ normalize_decimal('d.qt_material', 4, -1) }}             AS qt_material
    , {{ normalize_decimal('d.qt_dispensada', 4, -1) }}           AS qt_dispensada
    , {{ normalize_decimal('d.qt_devolvida', 4, -1) }}            AS qt_devolvida

    , {{ standardize_enum('d.ie_status', "'i'") }}                AS ie_status
    , {{ standardize_enum('d.ie_tipo_dispensacao', "'i'") }}      AS ie_tipo_dispensacao
    , {{ standardize_enum('d.ie_controlado', "'N'") }}            AS ie_controlado

    , {{ standardize_date('d.dh_dispensacao') }}                  AS dt_dispensacao
    , {{ standardize_date('d.dh_preparacao') }}                   AS dt_preparacao
    , {{ standardize_date('d.dh_cancelamento') }}                 AS dt_cancelamento

    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}      AS ds_observacao
    , {{ standardize_text_initcap('d.ds_justificativa', "'indefinido'") }}   AS ds_justificativa

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
