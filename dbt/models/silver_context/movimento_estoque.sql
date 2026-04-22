{#-
  Silver-Context: Movimento Estoque
  Movimento fisico de estoque consolidado com valorizacao (MOVIMENTO_ESTOQUE_VALOR),
  operacao (OPERACAO_ESTOQUE) e material. Foco em dispensacao ao paciente
  (IE_ORIGEM_DOCUMENTO=3 + CD_OPERACAO_ESTOQUE=3 = 92% dos movimentos).
  Grao: 1 linha por movimento de estoque (MOVIMENTO_ESTOQUE.NR_MOVIMENTO_ESTOQUE).
  PK: nr_movimento_estoque.
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "farmacia"]
  )
}}

WITH movimento AS (
  SELECT * FROM {{ ref('silver_tasy_movimento_estoque') }}
)
, valorizacao AS (
  -- Apenas o tipo de valor dominante (CD_TIPO_VALOR=7 = Custo medio ponderado, 99,26% dos registros)
  SELECT
      movimento_estoque_valor.nr_movimento_estoque                             AS nr_movimento_estoque
    , movimento_estoque_valor.vl_movimento                                     AS vl_movimento_custo
    , movimento_estoque_valor.vl_unitario                                      AS vl_unitario_custo
    , movimento_estoque_valor.vl_custo                                         AS vl_custo
    , movimento_estoque_valor.vl_total                                         AS vl_total_custo
  FROM {{ ref('silver_tasy_movimento_estoque_valor') }} AS movimento_estoque_valor
  WHERE movimento_estoque_valor.cd_tipo_valor = 7
)
, operacao AS (
  SELECT * FROM {{ ref('silver_tasy_operacao_estoque') }}
)
, material AS (
  SELECT * FROM {{ ref('silver_tasy_material') }}
)

SELECT
    movimento.nr_movimento_estoque                                             AS nr_movimento_estoque
  , movimento.cd_material                                                      AS cd_material
  , movimento.cd_operacao_estoque                                              AS cd_operacao_estoque
  , movimento.nr_prescricao                                                    AS nr_prescricao
  , movimento.nr_atendimento                                                   AS nr_atendimento
  , movimento.cd_local_estoque                                                 AS cd_local_estoque
  , movimento.cd_local_estoque_dest                                            AS cd_local_estoque_dest
  , movimento.cd_setor_atendimento                                             AS cd_setor_atendimento
  , movimento.cd_estabelecimento                                               AS cd_estabelecimento
  , movimento.nr_ordem_compra                                                  AS nr_ordem_compra
  , movimento.nr_lote_fabric                                                   AS nr_lote_fabric

    -- Operacao (desnormalizada)
  , operacao.ds_operacao_estoque                                               AS ds_operacao_estoque
  , operacao.ie_entrada_saida                                                  AS ie_entrada_saida_operacao
  , operacao.ie_tipo_operacao                                                  AS ie_tipo_operacao
  , operacao.ie_ajuste                                                         AS ie_ajuste_operacao
  , operacao.ie_transferencia                                                  AS ie_transferencia_operacao

    -- Material (desnormalizado minimo do cadastro mestre)
  , material.ds_material                                                       AS ds_material
  , material.ds_reduzida                                                       AS ds_material_reduzida
  , material.cd_grupo_material                                                 AS cd_grupo_material
  , material.ie_tipo_material                                                  AS ie_tipo_material
  , material.ie_controlado                                                     AS ie_controlado_material
  , material.ie_medicamento                                                    AS ie_medicamento_material

    -- Unidades
  , movimento.cd_unidade_medida                                                AS cd_unidade_medida
  , movimento.cd_unidade_medida_cons                                           AS cd_unidade_medida_cons

    -- Flags do movimento
  , movimento.ie_origem_documento                                              AS ie_origem_documento
  , movimento.ie_tipo_movimento                                                AS ie_tipo_movimento
  , movimento.ie_entrada_saida                                                 AS ie_entrada_saida
  , movimento.ie_situacao                                                      AS ie_situacao
  , movimento.ie_consignado                                                    AS ie_consignado

    -- Quantidades e valores
  , movimento.qt_movimento                                                     AS qt_movimento
  , movimento.qt_estoque                                                       AS qt_estoque
  , movimento.vl_movimento                                                     AS vl_movimento
  , movimento.vl_unitario                                                      AS vl_unitario
  , movimento.pr_desconto                                                      AS pr_desconto

    -- Valores consolidados da valorizacao (cd_tipo_valor=7, custo medio ponderado)
    -- Observacao: 99,26% dos movimentos tem cd_tipo_valor=7; 0,74% podem ficar NULL
  , valorizacao.vl_movimento_custo                                             AS vl_movimento_custo
  , valorizacao.vl_unitario_custo                                              AS vl_unitario_custo
  , valorizacao.vl_custo                                                       AS vl_custo
  , valorizacao.vl_total_custo                                                 AS vl_total_custo

    -- Timestamps
  , movimento.dt_movimento_estoque                                             AS dt_movimento_estoque
  , movimento.dt_mesano_referencia                                             AS dt_mesano_referencia
  , movimento.dt_validade                                                      AS dt_validade

    -- Derivada: tipo funcional do movimento (regras_negocio.md "Dispensacao efetiva no estoque")
    -- Dispensacao paciente = ie_origem_documento=3 AND cd_operacao_estoque=3 (92% dos movimentos)
  , CASE
      WHEN movimento.ie_origem_documento = '3' AND movimento.cd_operacao_estoque = 3
      THEN 'dispensacao_paciente'
      WHEN movimento.cd_operacao_estoque = 37 THEN 'devolucao_paciente'
      WHEN movimento.cd_operacao_estoque = 7  THEN 'entrada_nf_compra'
      WHEN operacao.ie_transferencia = 'S'    THEN 'transferencia'
      WHEN operacao.ie_entrada_saida = 'E'    THEN 'entrada_outra'
      WHEN operacao.ie_entrada_saida = 'S'    THEN 'saida_outra'
      ELSE 'outro'
    END                                                                        AS ie_tipo_funcional

    -- Observacoes
  , movimento.ds_observacao                                                    AS ds_observacao
  , movimento.ds_justificativa                                                 AS ds_justificativa

    -- Auditoria TASY
  , movimento.dt_atualizacao                                                   AS dt_atualizacao
  , movimento.nm_usuario                                                       AS nm_usuario
  , movimento.dt_atualizacao_nrec                                              AS dt_atualizacao_nrec
  , movimento.nm_usuario_nrec                                                  AS nm_usuario_nrec

  {{ silver_context_audit_columns() }}

FROM movimento
LEFT JOIN valorizacao
  ON valorizacao.nr_movimento_estoque = movimento.nr_movimento_estoque
LEFT JOIN operacao
  ON operacao.cd_operacao_estoque = movimento.cd_operacao_estoque
LEFT JOIN material
  ON material.cd_material = movimento.cd_material
