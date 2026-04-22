{#- Silver: one column per line + macros (dbt/macros/). PK: cd_material. -#}
{{
  config(
    materialized='incremental',
    schema='silver',
    unique_key='cd_material',
    incremental_strategy='merge',
    merge_update_columns=[
      'cd_grupo_material', 'cd_subgrupo_material', 'cd_classe_material',
      'cd_unidade_medida_estoque', 'cd_unidade_medida_compra', 'cd_unidade_medida_consumo',
      'cd_material_generico',
      'ds_material', 'ds_material_direto', 'ds_reduzida', 'ds_marca', 'ds_fabricante',
      'ds_principio_ativo', 'cd_barras',
      'ie_tipo_material', 'ie_situacao', 'ie_padronizado', 'ie_consignado',
      'ie_curva_abc', 'ie_classif_xyz', 'ie_alto_risco', 'ie_controlado',
      'ie_controle_medico', 'ie_restrito', 'ie_manipulado', 'ie_medicamento',
      'ie_antibiotico', 'ie_quimioterapico', 'ie_prescricao',
      'nr_registro_anvisa', 'cd_anvisa', 'cd_ean', 'cd_tuss',
      'dt_validade', 'dt_inicio_vigencia', 'dt_fim_vigencia',
      'dt_atualizacao', 'nm_usuario', 'dt_atualizacao_nrec', 'nm_usuario_nrec',
      '_is_deleted', '_cdc_op', '_cdc_event_at', '_cdc_ts_ms',
      '_bronze_row_hash', '_bronze_loaded_at', '_dbt_invocation_id'
    ],
    file_format='iceberg',
    on_schema_change='append_new_columns',
    tags=["silver", "tasy", "farmacia"],
    post_hook=post_hook_delete_source_tombstones('bronze_tasy_material', 'cd_material', '_is_deleted')
  )
}}

WITH base AS (
  SELECT * FROM {{ ref('bronze_tasy_material') }} b
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
          PARTITION BY b.cd_material
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
      d.cd_material                                               AS cd_material
    , {{ fill_null_bigint('d.cd_grupo_material', -1) }}           AS cd_grupo_material
    , {{ fill_null_bigint('d.cd_subgrupo_material', -1) }}        AS cd_subgrupo_material
    , {{ fill_null_bigint('d.cd_classe_material', -1) }}          AS cd_classe_material
    , {{ fill_null_bigint('d.cd_unidade_medida_estoque', -1) }}   AS cd_unidade_medida_estoque
    , {{ fill_null_bigint('d.cd_unidade_medida_compra', -1) }}    AS cd_unidade_medida_compra
    , {{ fill_null_bigint('d.cd_unidade_medida_consumo', -1) }}   AS cd_unidade_medida_consumo
    , {{ fill_null_bigint('d.cd_material_generico', -1) }}        AS cd_material_generico

      -- Descricoes
    , {{ standardize_text_initcap('d.ds_material', "'indefinido'") }}        AS ds_material
    , {{ standardize_text_initcap('d.ds_material_direto', "'indefinido'") }} AS ds_material_direto
    , {{ standardize_text_initcap('d.ds_reduzida', "'indefinido'") }}        AS ds_reduzida
    , {{ standardize_text_initcap('d.ds_marca', "'indefinido'") }}           AS ds_marca
    , {{ standardize_text_initcap('d.ds_fabricante', "'indefinido'") }}      AS ds_fabricante
    , {{ standardize_text_initcap('d.ds_principio_ativo', "'indefinido'") }} AS ds_principio_ativo
    , {{ fill_null_string('d.cd_barras', "'indefinido'") }}       AS cd_barras

      -- Categoricos / flags
      -- ie_tipo_material: 89% Medicamento (1), 7,6% Material (2), 1,4% Servico (5)
    , {{ standardize_enum('d.ie_tipo_material', "'i'") }}         AS ie_tipo_material
      -- ie_situacao: 85% I (inativos), 15% A (ativos) - filtro em Silver Context/Gold
    , {{ standardize_enum('d.ie_situacao', "'I'") }}              AS ie_situacao
    , {{ standardize_enum('d.ie_padronizado', "'N'") }}           AS ie_padronizado
    , {{ standardize_enum('d.ie_consignado', "'N'") }}            AS ie_consignado
    , {{ standardize_enum('d.ie_curva_abc', "'i'") }}             AS ie_curva_abc
    , {{ standardize_enum('d.ie_classif_xyz', "'i'") }}           AS ie_classif_xyz
    , {{ standardize_enum('d.ie_alto_risco', "'N'") }}            AS ie_alto_risco
    , {{ standardize_enum('d.ie_controlado', "'N'") }}            AS ie_controlado
    , {{ standardize_enum('d.ie_controle_medico', "'N'") }}       AS ie_controle_medico
    , {{ standardize_enum('d.ie_restrito', "'N'") }}              AS ie_restrito
    , {{ standardize_enum('d.ie_manipulado', "'N'") }}            AS ie_manipulado
    , {{ standardize_enum('d.ie_medicamento', "'N'") }}           AS ie_medicamento
    , {{ standardize_enum('d.ie_antibiotico', "'N'") }}           AS ie_antibiotico
    , {{ standardize_enum('d.ie_quimioterapico', "'N'") }}        AS ie_quimioterapico
    , {{ standardize_enum('d.ie_prescricao', "'N'") }}            AS ie_prescricao

      -- Atributos ANVISA / regulatorio (nr_registro_anvisa classificado como Morto em qualidade.md)
    , {{ fill_null_string('d.nr_registro_anvisa', "'indefinido'") }}  AS nr_registro_anvisa
    , {{ fill_null_string('d.cd_anvisa', "'indefinido'") }}           AS cd_anvisa
    , {{ fill_null_string('d.cd_ean', "'indefinido'") }}              AS cd_ean
    , {{ fill_null_string('d.cd_tuss', "'indefinido'") }}             AS cd_tuss

      -- Timestamps (dh_* -> dt_*)
    , {{ standardize_date('d.dh_validade') }}                     AS dt_validade
    , {{ standardize_date('d.dh_inicio_vigencia') }}              AS dt_inicio_vigencia
    , {{ standardize_date('d.dh_fim_vigencia') }}                 AS dt_fim_vigencia

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
