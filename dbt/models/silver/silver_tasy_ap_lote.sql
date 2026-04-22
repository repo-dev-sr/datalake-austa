{#- Silver: one column per line + macros (dbt/macros/). PK: nr_sequencia. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='nr_sequencia',
    incremental_strategy='merge',
    merge_update_columns=[
      'nr_prescricao', 'nr_atendimento', 'cd_setor_atendimento', 'cd_setor_ant',
      'nr_seq_classif', 'nr_seq_turno', 'nr_seq_lote_sup', 'nr_seq_embalagem',
      'nr_seq_motivo_parcial', 'nr_seq_mot_desdobrar', 'cd_estabelecimento',
      'ie_status_lote', 'ie_urgente', 'ie_controlado', 'ie_dispensacao', 'ie_tipo_lote',
      'dt_geracao_lote', 'dt_inicio_dispensacao', 'dt_atend_farmacia', 'dt_disp_farmacia', 'dt_entrega_setor',
      'dt_recebimento_setor', 'dt_impressao', 'dt_conferencia', 'dt_cancelamento',
      'dt_suspensao',
      'nm_usuario_atendimento', 'nm_usuario_disp', 'nm_usuario_entrega', 'nm_usuario_receb',
      'ds_observacao', 'ds_justificativa',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_ap_lote', 'nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_ap_lote') }} b
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
    , {{ fill_null_bigint('d.nr_atendimento', -1) }}              AS nr_atendimento
    , {{ fill_null_bigint('d.cd_setor_atendimento', -1) }}        AS cd_setor_atendimento
    , {{ fill_null_bigint('d.cd_setor_ant', -1) }}                AS cd_setor_ant
    , {{ fill_null_bigint('d.nr_seq_classif', -1) }}              AS nr_seq_classif
    , {{ fill_null_bigint('d.nr_seq_turno', -1) }}                AS nr_seq_turno
    , {{ fill_null_bigint('d.nr_seq_lote_sup', -1) }}             AS nr_seq_lote_sup
    , {{ fill_null_bigint('d.nr_seq_embalagem', -1) }}            AS nr_seq_embalagem
    , {{ fill_null_bigint('d.nr_seq_motivo_parcial', -1) }}       AS nr_seq_motivo_parcial
    , {{ fill_null_bigint('d.nr_seq_mot_desdobrar', -1) }}        AS nr_seq_mot_desdobrar
    , {{ fill_null_bigint('d.cd_estabelecimento', -1) }}          AS cd_estabelecimento

      -- Categoricos (dominio 2116 para ie_status_lote: 11 valores)
    , {{ standardize_enum('d.ie_status_lote', "'i'") }}           AS ie_status_lote
    , {{ standardize_enum('d.ie_urgente', "'N'") }}               AS ie_urgente
    , {{ standardize_enum('d.ie_controlado', "'N'") }}            AS ie_controlado
    , {{ standardize_enum('d.ie_dispensacao', "'i'") }}           AS ie_dispensacao
    , {{ standardize_enum('d.ie_tipo_lote', "'i'") }}             AS ie_tipo_lote

      -- Timestamps do ciclo SLA (dh_* -> dt_*)
    , {{ standardize_date('d.dh_geracao_lote') }}                 AS dt_geracao_lote
    , {{ standardize_date('d.dh_inicio_dispensacao') }}           AS dt_inicio_dispensacao
    , {{ standardize_date('d.dh_atend_farmacia') }}               AS dt_atend_farmacia
    , {{ standardize_date('d.dh_disp_farmacia') }}                AS dt_disp_farmacia
    , {{ standardize_date('d.dh_entrega_setor') }}                AS dt_entrega_setor
    , {{ standardize_date('d.dh_recebimento_setor') }}            AS dt_recebimento_setor
    , {{ standardize_date('d.dh_impressao') }}                    AS dt_impressao
    , {{ standardize_date('d.dh_conferencia') }}                  AS dt_conferencia
    , {{ standardize_date('d.dh_cancelamento') }}                 AS dt_cancelamento
    , {{ standardize_date('d.dh_suspensao') }}                    AS dt_suspensao

      -- Usuarios / observacoes
    , {{ standardize_text_initcap('d.nm_usuario_atendimento', "'indefinido'") }}  AS nm_usuario_atendimento
    , {{ standardize_text_initcap('d.nm_usuario_disp', "'indefinido'") }}         AS nm_usuario_disp
    , {{ standardize_text_initcap('d.nm_usuario_entrega', "'indefinido'") }}      AS nm_usuario_entrega
    , {{ standardize_text_initcap('d.nm_usuario_receb', "'indefinido'") }}        AS nm_usuario_receb
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}           AS ds_observacao
    , {{ standardize_text_initcap('d.ds_justificativa', "'indefinido'") }}        AS ds_justificativa

      -- Auditoria TASY
    , {{ standardize_date('d.dh_atualizacao') }}                  AS dt_atualizacao
    , {{ standardize_text_initcap('d.nm_usuario', "'indefinido'") }}              AS nm_usuario
    , {{ standardize_date('d.dh_atualizacao_nrec') }}             AS dt_atualizacao_nrec
    , {{ standardize_text_initcap('d.nm_usuario_nrec', "'indefinido'") }}         AS nm_usuario_nrec

    {{ silver_audit_columns() }}
  FROM latest_by_pk d
)

, final AS (
  SELECT *
  FROM shaped
  WHERE NOT _is_deleted
)

SELECT * FROM final
