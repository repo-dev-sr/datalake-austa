{#- Silver: one column per line + macros (dbt/macros/). PK composta: nr_prescricao + nr_sequencia. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key=['nr_prescricao', 'nr_sequencia'],
    incremental_strategy='merge',
    merge_update_columns=[
      'cd_procedimento', 'ie_origem_proced', 'cd_setor_atendimento', 'cd_medico_exec',
      'nr_seq_proc_interno',
      'ie_status_exec', 'ie_suspenso', 'ie_urgencia', 'ie_agrupador', 'ie_lado',
      'ie_amostra', 'ie_executar', 'ie_acm', 'ie_se_necessario',
      'qt_procedimento', 'qt_total',
      'dt_prev_execucao', 'dt_execucao', 'dt_suspensao', 'dt_lib_proc', 'dt_baixa',
      'dt_primeiro_horario',
      'ds_observacao', 'ds_justificativa',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_prescr_procedimento', 'nr_prescricao, nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_prescr_procedimento') }} b
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
          PARTITION BY b.nr_prescricao, b.nr_sequencia
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
    , d.nr_sequencia                                              AS nr_sequencia
    , {{ fill_null_bigint('d.cd_procedimento', -1) }}             AS cd_procedimento
    , {{ standardize_enum('d.ie_origem_proced', "'i'") }}         AS ie_origem_proced
    , {{ fill_null_bigint('d.cd_setor_atendimento', -1) }}        AS cd_setor_atendimento
    , {{ fill_null_bigint('d.cd_medico_exec', -1) }}              AS cd_medico_exec
    , {{ fill_null_bigint('d.nr_seq_proc_interno', -1) }}         AS nr_seq_proc_interno

    , {{ standardize_enum('d.ie_status_exec', "'i'") }}           AS ie_status_exec
    , {{ standardize_enum('d.ie_suspenso', "'N'") }}              AS ie_suspenso
    , {{ standardize_enum('d.ie_urgencia', "'N'") }}              AS ie_urgencia
    , {{ standardize_enum('d.ie_agrupador', "'i'") }}             AS ie_agrupador
    , {{ standardize_enum('d.ie_lado', "'i'") }}                  AS ie_lado
    , {{ standardize_enum('d.ie_amostra', "'N'") }}               AS ie_amostra
    , {{ standardize_enum('d.ie_executar', "'N'") }}              AS ie_executar
    , {{ standardize_enum('d.ie_acm', "'N'") }}                   AS ie_acm
    , {{ standardize_enum('d.ie_se_necessario', "'N'") }}         AS ie_se_necessario

    , {{ normalize_decimal('d.qt_procedimento', 4, -1) }}         AS qt_procedimento
    , {{ normalize_decimal('d.qt_total', 4, -1) }}                AS qt_total

    , {{ standardize_date('d.dh_prev_execucao') }}                AS dt_prev_execucao
    , {{ standardize_date('d.dh_execucao') }}                     AS dt_execucao
    , {{ standardize_date('d.dh_suspensao') }}                    AS dt_suspensao
    , {{ standardize_date('d.dh_lib_proc') }}                     AS dt_lib_proc
    , {{ standardize_date('d.dh_baixa') }}                        AS dt_baixa
    , {{ standardize_date('d.dh_primeiro_horario') }}             AS dt_primeiro_horario

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
