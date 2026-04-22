{#- Silver: one column per line + macros (dbt/macros/). PK: nr_prescricao. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_prescricao',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_atendimento', 'cd_medico', 'cd_setor_atendimento', 'cd_pessoa_fisica',
      'cd_estabelecimento', 'nr_seq_agrupador', 'nr_prescricao_original', 'cd_funcao_origem',
      'ie_origem_inf', 'ie_adep', 'ie_prescr_emergencia', 'ie_recem_nato',
      'ie_lib_farm', 'ie_hemodialise', 'ie_tipo_prescricao', 'ie_situacao', 'ie_retroativa',
      'dt_prescricao', 'dt_liberacao', 'dt_liberacao_medico', 'dt_liberacao_farmacia',
      'dt_suspensao', 'dt_validade_prescr', 'dt_entrega', 'dt_primeiro_horario',
      'ds_observacao', 'ds_justificativa',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_prescr_medica', 'nr_prescricao', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_prescr_medica') }} b
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
          PARTITION BY b.nr_prescricao
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
      d.nr_prescricao                                             AS nr_prescricao
    , {{ fill_null_bigint('d.nr_atendimento', -1) }}              AS nr_atendimento
    , {{ fill_null_bigint('d.cd_medico', -1) }}                   AS cd_medico
    , {{ fill_null_bigint('d.cd_setor_atendimento', -1) }}        AS cd_setor_atendimento
    , {{ fill_null_bigint('d.cd_pessoa_fisica', -1) }}            AS cd_pessoa_fisica
    , {{ fill_null_bigint('d.cd_estabelecimento', -1) }}          AS cd_estabelecimento
    , {{ fill_null_bigint('d.nr_seq_agrupador', -1) }}            AS nr_seq_agrupador
    , {{ fill_null_bigint('d.nr_prescricao_original', -1) }}      AS nr_prescricao_original
      -- cd_funcao_origem: 4,7% NULL observado na qualidade.md, principais: 2314 (86,4%), 916 (6%)
    , {{ fill_null_bigint('d.cd_funcao_origem', -1) }}            AS cd_funcao_origem

      -- Categoricos / flags
    , {{ standardize_enum('d.ie_origem_inf', "'i'") }}            AS ie_origem_inf
    , {{ standardize_enum('d.ie_adep', "'N'") }}                  AS ie_adep
    , {{ standardize_enum('d.ie_prescr_emergencia', "'N'") }}     AS ie_prescr_emergencia
    , {{ standardize_enum('d.ie_recem_nato', "'N'") }}            AS ie_recem_nato
    , {{ standardize_enum('d.ie_lib_farm', "'N'") }}              AS ie_lib_farm
    , {{ standardize_enum('d.ie_hemodialise', "'N'") }}           AS ie_hemodialise
    , {{ standardize_enum('d.ie_tipo_prescricao', "'i'") }}       AS ie_tipo_prescricao
    , {{ standardize_enum('d.ie_situacao', "'i'") }}              AS ie_situacao
    , {{ standardize_enum('d.ie_retroativa', "'N'") }}            AS ie_retroativa

      -- Timestamps (dh_* -> dt_*)
    , {{ standardize_date('d.dh_prescricao') }}                   AS dt_prescricao
      -- dt_liberacao: 1,9% NULL. Filtro de rascunho 'dt_liberacao IS NOT NULL' aplicado em Silver Context/Gold
    , {{ standardize_date('d.dh_liberacao') }}                    AS dt_liberacao
    , {{ standardize_date('d.dh_liberacao_medico') }}             AS dt_liberacao_medico
      -- dt_liberacao_farmacia: 57,5% NULL bruto; NULL esperado para prescricoes sem medicamento
    , {{ standardize_date('d.dh_liberacao_farmacia') }}           AS dt_liberacao_farmacia
    , {{ standardize_date('d.dh_suspensao') }}                    AS dt_suspensao
    , {{ standardize_date('d.dh_validade_prescr') }}              AS dt_validade_prescr
    , {{ standardize_date('d.dh_entrega') }}                      AS dt_entrega
    , {{ standardize_date('d.dh_primeiro_horario') }}             AS dt_primeiro_horario

      -- Observacoes
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}      AS ds_observacao
    , {{ standardize_text_initcap('d.ds_justificativa', "'indefinido'") }}   AS ds_justificativa

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
