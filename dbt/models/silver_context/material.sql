{#-
  Silver-Context: Material
  Cadastro mestre de material/medicamento consolidado a partir de SILVER_TASY_MATERIAL.
  Grao: 1 linha por material (cd_material).
  PK: cd_material.
  Observacao: 85% dos registros sao inativos (ie_situacao='I') e 15% ativos ('A').
  Filtro de ativos aplicado em Gold via dim_medicamento (camada de dimensao).
  Aqui preservamos todos os materiais - e apenas a consolidacao de negocio.
-#}
{{
  config(
    materialized='table'
    , schema='silver_context'
    , file_format='iceberg'
    , tags=["silver_context", "tasy", "farmacia"]
  )
}}

WITH material AS (
  SELECT * FROM {{ ref('silver_tasy_material') }}
)

SELECT
    material.cd_material                                                       AS cd_material
  , material.cd_grupo_material                                                 AS cd_grupo_material
  , material.cd_subgrupo_material                                              AS cd_subgrupo_material
  , material.cd_classe_material                                                AS cd_classe_material
  , material.cd_material_generico                                              AS cd_material_generico

    -- Unidades padrao
  , material.cd_unidade_medida_estoque                                         AS cd_unidade_medida_estoque
  , material.cd_unidade_medida_compra                                          AS cd_unidade_medida_compra
  , material.cd_unidade_medida_consumo                                         AS cd_unidade_medida_consumo

    -- Descritivos
  , material.ds_material                                                       AS ds_material
  , material.ds_material_direto                                                AS ds_material_direto
  , material.ds_reduzida                                                       AS ds_reduzida
  , material.ds_marca                                                          AS ds_marca
  , material.ds_fabricante                                                     AS ds_fabricante
  , material.ds_principio_ativo                                                AS ds_principio_ativo
  , material.cd_barras                                                         AS cd_barras

    -- Codigos regulatorios
  , material.nr_registro_anvisa                                                AS nr_registro_anvisa
  , material.cd_anvisa                                                         AS cd_anvisa
  , material.cd_ean                                                            AS cd_ean
  , material.cd_tuss                                                           AS cd_tuss

    -- Tipo e classificacoes
    -- ie_tipo_material: 89% Medicamento (1), 7,6% Material (2), 1,4% Servico (5)
  , material.ie_tipo_material                                                  AS ie_tipo_material
  , material.ie_situacao                                                       AS ie_situacao
  , material.ie_padronizado                                                    AS ie_padronizado
  , material.ie_consignado                                                     AS ie_consignado
  , material.ie_curva_abc                                                      AS ie_curva_abc
  , material.ie_classif_xyz                                                    AS ie_classif_xyz

    -- Flags clinicas
  , material.ie_alto_risco                                                     AS ie_alto_risco
  , material.ie_controlado                                                     AS ie_controlado
  , material.ie_controle_medico                                                AS ie_controle_medico
  , material.ie_restrito                                                       AS ie_restrito
  , material.ie_manipulado                                                     AS ie_manipulado
  , material.ie_medicamento                                                    AS ie_medicamento
  , material.ie_antibiotico                                                    AS ie_antibiotico
  , material.ie_quimioterapico                                                 AS ie_quimioterapico
  , material.ie_prescricao                                                     AS ie_prescricao

    -- Derivada: classificacao funcional de negocio
  , CASE
      WHEN material.ie_quimioterapico = 'S' THEN 'quimioterapico'
      WHEN material.ie_antibiotico = 'S'    THEN 'antibiotico'
      WHEN material.ie_controlado = 'S'     THEN 'controlado'
      WHEN material.ie_alto_risco = 'S'     THEN 'alto_risco'
      WHEN material.ie_medicamento = 'S'    THEN 'medicamento_comum'
      WHEN material.ie_tipo_material = '2'  THEN 'material_hospitalar'
      WHEN material.ie_tipo_material = '5'  THEN 'servico'
      ELSE 'outro'
    END                                                                        AS ie_categoria_funcional

  , CASE
      WHEN material.ie_situacao = 'A' THEN 'S'
      ELSE 'N'
    END                                                                        AS ie_ativo

    -- Validade / vigencia
  , material.dt_validade                                                       AS dt_validade
  , material.dt_inicio_vigencia                                                AS dt_inicio_vigencia
  , material.dt_fim_vigencia                                                   AS dt_fim_vigencia

    -- Auditoria TASY
  , material.dt_atualizacao                                                    AS dt_atualizacao
  , material.nm_usuario                                                        AS nm_usuario
  , material.dt_atualizacao_nrec                                               AS dt_atualizacao_nrec
  , material.nm_usuario_nrec                                                   AS nm_usuario_nrec

  {{ silver_context_audit_columns() }}

FROM material
