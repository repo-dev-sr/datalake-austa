{{
  config(
    materialized='incremental'
    , schema='bronze'
    , file_format='iceberg'
    , incremental_strategy='append'
    , on_schema_change='append_new_columns'
  )
}}

{% set raw_path = "s3a://austa-lakehouse-prod-data-lake-169446931765/raw/raw-tasy/stream/austa.TASY.PLS_LOTE_MENSALIDADE/" %}
{% set cdc_lookback_hours = var('cdc_lookback_hours', 2) %}
{% set cdc_reprocess_hours = var('cdc_reprocess_hours', 0) %}

WITH target_watermark AS (
  {% if is_incremental() %}
    SELECT CAST(COALESCE(MAX(_cdc_ts_ms), 0) AS BIGINT) AS max_ts_ms
    FROM {{ this }}
  {% else %}
    SELECT CAST(0 AS BIGINT) AS max_ts_ms
  {% endif %}
)
, params AS (
  SELECT
    CASE
      WHEN {{ cdc_reprocess_hours }} > 0 THEN
        (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - ({{ cdc_reprocess_hours }} * 3600)) * 1000
      ELSE
        GREATEST(
          0
          , (SELECT max_ts_ms FROM target_watermark) - ({{ cdc_lookback_hours }} * 3600 * 1000)
        )
    end  AS wm_start_ms
)
{{ bronze_raw_incremental_austa_envelope(raw_path) }}
SELECT
    r.nr_sequencia
  , r.dt_atualizacao as dh_atualizacao
  , r.nm_usuario
  , r.dt_atualizacao_nrec as dh_atualizacao_nrec
  , r.nm_usuario_nrec
  , r.dt_mesano_referencia as dh_mesano_referencia
  , r.ie_status
  , r.dt_geracao as dh_geracao
  , r.ie_tipo_contratacao
  , r.ie_regulamentacao
  , r.ie_fator_moderador
  , r.ie_participacao
  , r.nr_seq_contrato
  , r.cd_estabelecimento
  , r.dt_liberacao as dh_liberacao
  , r.vl_lote
  , r.dt_geracao_titulos as dh_geracao_titulos
  , r.dt_geracao_nf as dh_geracao_nf
  , r.vl_pre_estabelecido
  , r.vl_pos_estabelecido
  , r.vl_coparticipacao
  , r.vl_outros
  , r.vl_adicionais
  , r.vl_cancelado
  , r.ds_observacao
  , r.ie_primeira_mensalidade
  , r.nr_lote_contabil
  , r.nr_seq_pagador
  , r.nr_lote_contab_antecip
  , r.dt_contabilizacao as dh_contabilizacao
  , r.nr_dia_inicial_venc
  , r.nr_dia_final_venc
  , r.dt_rescisao_programada as dh_rescisao_programada
  , r.nr_seq_forma_cobranca
  , r.cd_banco
  , r.ie_cobrar_retroativo
  , r.nr_contrato
  , r.nr_seq_empresa
  , r.dt_inicio_geracao as dh_inicio_geracao
  , r.dt_fim_geracao as dh_fim_geracao
  , r.qt_beneficiario_lote
  , r.vl_pro_rata_dia
  , r.vl_antecipacao
  , r.qt_pagadores_lote
  , r.cd_tipo_portador
  , r.cd_portador
  , r.dt_inicio_adesao as dh_inicio_adesao
  , r.dt_fim_adesao as dh_fim_adesao
  , r.nr_seq_contrato_inter
  , r.ie_gerar_mensalidade_futura
  , r.qt_meses_mensalidade_futura
  , r.nr_seq_grupo_contrato
  , r.dt_inicio_status as dh_inicio_status
  , r.dt_fim_status as dh_fim_status
  , r.dt_inicio_geracao_titulo as dh_inicio_geracao_titulo
  , r.dt_fim_geracao_titulo as dh_fim_geracao_titulo
  , r.ie_tipo_lote
  , r.ie_tipo_preco
  , r.nr_seq_regra_grupo
  , r.hr_geracao_lote
  , r.ie_mensalidade_mes_anterior
  , r.nr_seq_grupo_preco
  , r.cd_empresa_inicial
  , r.cd_empresa_final
  , r.cd_perfil
  , r.ie_tipo_contrato
  , r.nr_seq_regra_serie_nf
  , r.nr_seq_grupo_inter
  , r.nr_seq_classif_itens
  , r.ie_pagador_beneficio_obito
  , r.ie_geracao_nota_titulo
  , r.ie_endereco_boleto
  , r.ie_tipo_pessoa_pagador
  , r.dt_liberacao_dist as dh_liberacao_dist
  , r.nm_usuario_dist
  , r.nr_seq_classif_benef
  , r.ie_visualizar_portal
  , r.nr_seq_segurado
  , r.ie_mens_ant_agrupar
  , r.ie_mens_ant_mes_atual
  , r.ie_envia_cobranca
  , r.ie_utilizacao
  , r.cd_classif_contrato
  , r.dt_contrato_inicial as dh_contrato_inicial
  , r.dt_contrato_final as dh_contrato_final
  , r.nr_seq_solic_resc_fin
  , r.nr_seq_motivo_suspensao
  , r.dt_susp_mens_inicial as dh_susp_mens_inicial
  , r.dt_susp_mens_final as dh_susp_mens_final
  , r.nr_seq_lote_mov_mens
  , r.nm_usuario_geracao
  , r.nr_seq_grupo_itens
  , r.dt_liberacao_inicial as dh_liberacao_inicial
  , r.dt_liberacao_final as dh_liberacao_final
  , r.ds_timezone
  {{ bronze_audit_columns(raw_path) }}
  , r.__source_txid
FROM raw_incremental r
