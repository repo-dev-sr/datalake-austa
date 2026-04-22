{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{#- Receitas ambulatoriais - 2.696 registros -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_atendimento', 'cd_pessoa_fisica', 'cd_medico', 'cd_estabelecimento',
      'nr_receita', 'ie_tipo_receita', 'ie_situacao', 'ie_controlada',
      'dt_receita', 'dt_validade', 'dt_dispensacao',
      'ds_observacao',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_fa_receita_farmacia', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_fa_receita_farmacia') }} b
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
    , {{ fill_null_bigint('d.cd_medico', -1) }}                   AS cd_medico
    , {{ fill_null_bigint('d.cd_estabelecimento', -1) }}          AS cd_estabelecimento

    , {{ fill_null_string('d.nr_receita', "'indefinido'") }}      AS nr_receita
    , {{ standardize_enum('d.ie_tipo_receita', "'i'") }}          AS ie_tipo_receita
    , {{ standardize_enum('d.ie_situacao', "'i'") }}              AS ie_situacao
    , {{ standardize_enum('d.ie_controlada', "'N'") }}            AS ie_controlada

    , {{ standardize_date('d.dh_receita') }}                      AS dt_receita
    , {{ standardize_date('d.dh_validade') }}                     AS dt_validade
    , {{ standardize_date('d.dh_dispensacao') }}                  AS dt_dispensacao

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
