{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{#- Historico analise GPT - ALERTA: NR_PRESCRICAO e IE_STATUS_ANALISE_FARM 100% NULL em qualidade.md -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_atendimento', 'cd_pessoa_fisica', 'cd_resp_analise', 'nr_prescricao',
      'ie_tipo_analise', 'ie_status_analise', 'ie_status_analise_farm', 'ie_resultado',
      'ds_analise', 'ds_conduta', 'ds_observacao',
      'dt_analise', 'dt_liberacao',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_gpt_hist_analise_plano', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_gpt_hist_analise_plano') }} b
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
    , {{ fill_null_bigint('d.cd_pessoa_fisica', -1) }}            AS cd_pessoa_fisica
    , {{ fill_null_bigint('d.cd_resp_analise', -1) }}             AS cd_resp_analise
      -- nr_prescricao: 100% NULL em qualidade.md - FK morta, preservada como -1
    , {{ fill_null_bigint('d.nr_prescricao', -1) }}               AS nr_prescricao

    , {{ standardize_enum('d.ie_tipo_analise', "'i'") }}          AS ie_tipo_analise
    , {{ standardize_enum('d.ie_status_analise', "'i'") }}        AS ie_status_analise
      -- ie_status_analise_farm: 100% NULL em qualidade.md - Morto
    , {{ standardize_enum('d.ie_status_analise_farm', "'i'") }}   AS ie_status_analise_farm
    , {{ standardize_enum('d.ie_resultado', "'i'") }}             AS ie_resultado
    , {{ standardize_text_initcap('d.ds_analise', "'indefinido'") }}         AS ds_analise
    , {{ standardize_text_initcap('d.ds_conduta', "'indefinido'") }}         AS ds_conduta
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}      AS ds_observacao

    , {{ standardize_date('d.dh_analise') }}                      AS dt_analise
    , {{ standardize_date('d.dh_liberacao') }}                    AS dt_liberacao

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
