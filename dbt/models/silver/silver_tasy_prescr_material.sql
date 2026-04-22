{#- Silver: one column per line + macros (dbt/macros/). PK composta: nr_prescricao + nr_sequencia. -#}
{#- unique_key como lista - dbt-spark merge com multiplas colunas -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key=['nr_prescricao', 'nr_sequencia'],
    incremental_strategy='merge',
    merge_update_columns=[
      'cd_material', 'cd_unidade_medida', 'cd_unidade_medida_dose', 'cd_intervalo',
      'nr_seq_solucao', 'nr_seq_superior',
      'ie_agrupador', 'ie_suspenso', 'ie_acm', 'ie_urgencia', 'ie_se_necessario',
      'ie_via_aplicacao', 'ie_administrar', 'ie_controlado', 'ie_origem_inf',
      'qt_dose', 'qt_unitaria', 'qt_total_dispensar', 'qt_material', 'qt_dispensar',
      'qt_conversao_dose',
      'dt_inicio_medic', 'dt_fim_medic', 'dt_suspensao', 'dt_lib_material',
      'dt_lib_farmacia', 'dt_primeiro_horario', 'dt_validade',
      'ds_observacao', 'ds_justificativa', 'ds_dose_diferenciada',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_prescr_material', 'nr_prescricao, nr_sequencia', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_prescr_material') }} b
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
    , {{ fill_null_bigint('d.cd_material', -1) }}                 AS cd_material
    , {{ fill_null_bigint('d.cd_unidade_medida', -1) }}           AS cd_unidade_medida
    , {{ fill_null_bigint('d.cd_unidade_medida_dose', -1) }}      AS cd_unidade_medida_dose
    , {{ fill_null_bigint('d.cd_intervalo', -1) }}                AS cd_intervalo
    , {{ fill_null_bigint('d.nr_seq_solucao', -1) }}              AS nr_seq_solucao
    , {{ fill_null_bigint('d.nr_seq_superior', -1) }}             AS nr_seq_superior

      -- Categoricos - ie_agrupador dominio 1131 (17 valores, 1=Medicamento, 2=Material)
    , {{ standardize_enum('d.ie_agrupador', "'i'") }}             AS ie_agrupador
    , {{ standardize_enum('d.ie_suspenso', "'N'") }}              AS ie_suspenso
    , {{ standardize_enum('d.ie_acm', "'N'") }}                   AS ie_acm
    , {{ standardize_enum('d.ie_urgencia', "'N'") }}              AS ie_urgencia
    , {{ standardize_enum('d.ie_se_necessario', "'N'") }}         AS ie_se_necessario
    , {{ standardize_enum('d.ie_via_aplicacao', "'i'") }}         AS ie_via_aplicacao
    , {{ standardize_enum('d.ie_administrar', "'N'") }}           AS ie_administrar
    , {{ standardize_enum('d.ie_controlado', "'N'") }}            AS ie_controlado
    , {{ standardize_enum('d.ie_origem_inf', "'i'") }}            AS ie_origem_inf

      -- Quantidades
    , {{ normalize_decimal('d.qt_dose', 4, -1) }}                 AS qt_dose
    , {{ normalize_decimal('d.qt_unitaria', 4, -1) }}             AS qt_unitaria
      -- QT_TOTAL_DISPENSAR: outlier max 2.326.694 -> aplicar CASE > 10000 -> NULL (regras_negocio.md)
    , CASE
        WHEN d.qt_total_dispensar > 10000 THEN NULL
        ELSE {{ normalize_decimal('d.qt_total_dispensar', 4, -1) }}
      END                                                         AS qt_total_dispensar
    , {{ normalize_decimal('d.qt_material', 4, -1) }}             AS qt_material
      -- QT_DISPENSAR: 52 negativos observados -> CASE < 0 -> NULL
    , CASE
        WHEN d.qt_dispensar < 0 THEN NULL
        ELSE {{ normalize_decimal('d.qt_dispensar', 4, -1) }}
      END                                                         AS qt_dispensar
    , {{ normalize_decimal('d.qt_conversao_dose', 4, -1) }}       AS qt_conversao_dose

      -- Timestamps (dh_* -> dt_*)
    , {{ standardize_date('d.dh_inicio_medic') }}                 AS dt_inicio_medic
    , {{ standardize_date('d.dh_fim_medic') }}                    AS dt_fim_medic
    , {{ standardize_date('d.dh_suspensao') }}                    AS dt_suspensao
      -- Cascata de liberacao: usar dt_lib_material/dt_lib_farmacia em Silver Context
    , {{ standardize_date('d.dh_lib_material') }}                 AS dt_lib_material
    , {{ standardize_date('d.dh_lib_farmacia') }}                 AS dt_lib_farmacia
    , {{ standardize_date('d.dh_primeiro_horario') }}             AS dt_primeiro_horario
    , {{ standardize_date('d.dh_validade') }}                     AS dt_validade

      -- Observacoes
    , {{ standardize_text_initcap('d.ds_observacao', "'indefinido'") }}         AS ds_observacao
    , {{ standardize_text_initcap('d.ds_justificativa', "'indefinido'") }}      AS ds_justificativa
    , {{ standardize_text_initcap('d.ds_dose_diferenciada', "'indefinido'") }}  AS ds_dose_diferenciada

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
