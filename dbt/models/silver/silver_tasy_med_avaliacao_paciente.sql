{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{#- Avaliacoes clinicas - tipo 2071 e 'Indicador Farmaceutico' (view AMH_INDICADOR_FARMA_V) -#}
{#- ALERTA: 0 registros tipo 2071 em homologacao, validar em producao -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_atendimento', 'cd_pessoa_fisica', 'cd_profissional', 'nr_seq_tipo_avaliacao',
      'nr_seq_resultado', 'nr_seq_intervencao',
      'ie_tipo_avaliacao', 'ie_status', 'ie_resultado',
      'ds_avaliacao', 'ds_observacao', 'ds_conduta',
      'dt_avaliacao', 'dt_liberacao',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_med_avaliacao_paciente', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_med_avaliacao_paciente') }} b
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
    , {{ fill_null_bigint('d.cd_profissional', -1) }}             AS cd_profissional
      -- nr_seq_tipo_avaliacao: 2071 = Indicador Farmaceutico (chave do dominio)
    , {{ fill_null_bigint('d.nr_seq_tipo_avaliacao', -1) }}       AS nr_seq_tipo_avaliacao
    , {{ fill_null_bigint('d.nr_seq_resultado', -1) }}            AS nr_seq_resultado
    , {{ fill_null_bigint('d.nr_seq_intervencao', -1) }}          AS nr_seq_intervencao

    , {{ standardize_enum('d.ie_tipo_avaliacao', "'i'") }}        AS ie_tipo_avaliacao
    , {{ standardize_enum('d.ie_status', "'i'") }}                AS ie_status
    , {{ standardize_enum('d.ie_resultado', "'i'") }}             AS ie_resultado
    , {{ standardize_text_initcap('d.ds_avaliacao', "'indefinido'") }}       AS ds_avaliacao
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}      AS ds_observacao
    , {{ standardize_text_initcap('d.ds_conduta', "'indefinido'") }}         AS ds_conduta

    , {{ standardize_date('d.dh_avaliacao') }}                    AS dt_avaliacao
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
