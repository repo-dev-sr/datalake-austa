{#-
  Silver-Context: Consumo Material
  Registro de cada material/medicamento consumido/cobrado por atendimento -
  consolida MATERIAL_ATEND_PACIENTE com cadastro de material. Base para analises
  de custo, margem, consumo por convenio/setor e produtividade.
  Grao: 1 linha por material dispensado ao paciente (MATERIAL_ATEND_PACIENTE.NR_SEQUENCIA).
  PK: nr_seq_consumo.
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "farmacia"]
  )
}}

WITH material_atend AS (
  SELECT * FROM {{ ref('silver_tasy_material_atend_paciente') }}
)
, material AS (
  SELECT * FROM {{ ref('silver_tasy_material') }}
)

SELECT
    material_atend.nr_sequencia                                                AS nr_seq_consumo
  , material_atend.nr_atendimento                                              AS nr_atendimento
  , material_atend.nr_prescricao                                               AS nr_prescricao
  , material_atend.nr_sequencia_prescricao                                     AS nr_sequencia_prescricao
  , material_atend.cd_setor_atendimento                                        AS cd_setor_atendimento
  , material_atend.cd_estabelecimento                                          AS cd_estabelecimento
  , material_atend.cd_medico                                                   AS cd_medico
  , material_atend.cd_convenio                                                 AS cd_convenio
  , material_atend.cd_categoria                                                AS cd_categoria

    -- Material (desnormalizado)
  , material_atend.cd_material                                                 AS cd_material
  , material.ds_material                                                       AS ds_material
  , material.ds_reduzida                                                       AS ds_material_reduzida
  , material.ds_principio_ativo                                                AS ds_principio_ativo
  , material.cd_grupo_material                                                 AS cd_grupo_material
  , material.cd_subgrupo_material                                              AS cd_subgrupo_material
  , material.cd_classe_material                                                AS cd_classe_material
  , material.ie_tipo_material                                                  AS ie_tipo_material
  , material.ie_controlado                                                     AS ie_controlado_material
  , material.ie_alto_risco                                                     AS ie_alto_risco_material
  , material.ie_medicamento                                                    AS ie_medicamento_material
  , material.ie_antibiotico                                                    AS ie_antibiotico_material
  , material.ie_quimioterapico                                                 AS ie_quimioterapico_material
  , material.ie_padronizado                                                    AS ie_padronizado_material
  , material.ie_curva_abc                                                      AS ie_curva_abc_material
  , material.ie_situacao                                                       AS ie_situacao_material

    -- Precificacao
  , material_atend.cd_tab_preco_material                                       AS cd_tab_preco_material
  , material_atend.nr_seq_regra_ajuste_mat                                     AS nr_seq_regra_ajuste_mat
  , material_atend.cd_unidade_medida                                           AS cd_unidade_medida

    -- Quantidades e valores (custos + cobranca)
  , material_atend.qt_material                                                 AS qt_material
  , material_atend.vl_material                                                 AS vl_material
  , material_atend.vl_unitario                                                 AS vl_unitario
  , material_atend.vl_custo                                                    AS vl_custo
  , material_atend.vl_custo_medio                                              AS vl_custo_medio
  , material_atend.vl_convenio                                                 AS vl_convenio
  , material_atend.pr_desconto                                                 AS pr_desconto

    -- Derivada: margem bruta (cobrado - custo). Valores -1 sao sentinelas de NULL,
    -- portanto so calculamos quando ambos existirem > 0
  , CASE
      WHEN material_atend.vl_convenio > 0 AND material_atend.vl_custo_medio > 0
      THEN CAST(material_atend.vl_convenio - material_atend.vl_custo_medio AS DECIMAL(18,2))
      ELSE NULL
    END                                                                        AS vl_margem_bruta

    -- Flags
  , material_atend.ie_situacao                                                 AS ie_situacao
  , material_atend.ie_valor_informado                                          AS ie_valor_informado
  , material_atend.ie_tipo_material                                            AS ie_tipo_material_atend
  , material_atend.ie_cobranca                                                 AS ie_cobranca
  , material_atend.ie_suspenso                                                 AS ie_suspenso

    -- Timestamps
  , material_atend.dt_atendimento                                              AS dt_atendimento
  , material_atend.dt_conta                                                    AS dt_conta
  , material_atend.dt_validade                                                 AS dt_validade

    -- Observacoes
  , material_atend.ds_observacao                                               AS ds_observacao
  , material_atend.ds_complemento                                              AS ds_complemento

    -- Auditoria TASY
  , material_atend.dt_atualizacao                                              AS dt_atualizacao
  , material_atend.nm_usuario                                                  AS nm_usuario
  , material_atend.dt_atualizacao_nrec                                         AS dt_atualizacao_nrec
  , material_atend.nm_usuario_nrec                                             AS nm_usuario_nrec

  {{ silver_context_audit_columns() }}

FROM material_atend
LEFT JOIN material
  ON material.cd_material = material_atend.cd_material
