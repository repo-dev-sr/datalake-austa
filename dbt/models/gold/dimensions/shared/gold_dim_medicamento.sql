-- Grain: 1 linha = 1 material/medicamento do catálogo TASY (deduplicado por cd_material)
-- Fonte: silver_context.material via {{ ref('material') }}
-- Camada: Gold | Tipo: SCD1
-- Dimensão conformada — compartilhada entre fatos de farmácia (dispensacao, volume, consumo, movimento estoque)
-- Observação: inclui ativos e inativos. Filtros de ativação (ie_situacao='A') devem ocorrer
--             na camada de consumo (dashboard), preservando rastreabilidade histórica.
{{
  config(
      materialized='table'
    , schema='gold'
    , file_format='iceberg'
    , tags=["gold", "tasy", "medicamento", "dimensao_conformada"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('material') }}
    WHERE cd_material <> -1
)

, final AS (
    SELECT
        -- ── Surrogate key ────────────────────────────────────────────────────────
          {{ dbt_utils.generate_surrogate_key(['cd_material']) }}                AS sk_medicamento

        -- ── Natural key preservada ───────────────────────────────────────────────
        , cd_material                                                            AS nk_medicamento

        -- ── Hierarquia merceológica ──────────────────────────────────────────────
        , cd_grupo_material
        , cd_subgrupo_material
        , cd_classe_material
        , cd_material_generico

        -- ── Descritivos ──────────────────────────────────────────────────────────
        , ds_material
        , ds_reduzida
        , ds_principio_ativo
        , ds_marca
        , ds_fabricante

        -- ── Codigos regulatórios ─────────────────────────────────────────────────
        , nr_registro_anvisa
        , cd_anvisa
        , cd_ean
        , cd_tuss
        , cd_barras

        -- ── Unidades padrão ──────────────────────────────────────────────────────
        , cd_unidade_medida_estoque
        , cd_unidade_medida_compra
        , cd_unidade_medida_consumo

        -- ── Tipo e situação ──────────────────────────────────────────────────────
        , ie_tipo_material
        , ie_situacao
        , ie_ativo
        , ie_padronizado
        , ie_consignado
        , ie_curva_abc
        , ie_classif_xyz

        -- ── Flags clínicas ───────────────────────────────────────────────────────
        , ie_medicamento
        , ie_controlado
        , ie_controle_medico
        , ie_alto_risco
        , ie_antibiotico
        , ie_quimioterapico
        , ie_restrito
        , ie_manipulado
        , ie_prescricao

        -- ── Categoria funcional derivada na Silver-Context ───────────────────────
        , ie_categoria_funcional

        -- ── Vigência ─────────────────────────────────────────────────────────────
        , dt_validade
        , dt_inicio_vigencia
        , dt_fim_vigencia

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')           AS _gold_loaded_at

    FROM source
)

SELECT * FROM final
