-- Grain: 1 linha = 1 código CID (deduplicado por cd_doenca_cid)
-- Fonte: silver_context.procedimento via {{ ref('procedimento') }}
-- Camada: Gold | Tipo: SCD1 (códigos CID são estáveis)
-- Dimensão conformada — compartilhada entre fatos assistenciais e financeiros
-- ⚠️  Apenas código disponível — sem descrição no Silver-Context atual.
--     Enriquecer com seed de tabela CID quando disponível.
{{
  config(
    materialized='table'
    , schema='gold'
    , file_format='iceberg'
    , tags=["gold", "tasy", "cid", "dimensao_conformada"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('procedimento') }}
    WHERE cd_doenca_cid <> -1
)

-- Pega o estado mais recente de cada CID via _context_processed_at
, cid_ranked AS (
    SELECT
        cd_doenca_cid
      , ROW_NUMBER() OVER (
            PARTITION BY cd_doenca_cid
            ORDER BY _context_processed_at DESC
        ) AS rn
    FROM source
)

, final AS (
    SELECT
          {{ dbt_utils.generate_surrogate_key(['cd_doenca_cid']) }}       AS sk_cid
        , cd_doenca_cid                                                   AS nk_cid
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')   AS _gold_loaded_at
    FROM cid_ranked
    WHERE rn = 1
)

SELECT * FROM final
