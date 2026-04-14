-- Grain: 1 linha = 1 dia do calendário gregoriano
-- Fonte: gerada via Spark SEQUENCE — sem dependência de Silver-Context
-- Camada: Gold | Tipo: SCD1 | Dimensão conformada global — compartilhada entre todos os fatos
-- Range configurável em dbt_project.yml:
--   dim_tempo_start_date (padrão: 2010-01-01)
--   dim_tempo_end_date   (padrão: 2030-01-01)
-- Para expandir o range: atualizar dim_tempo_end_date e executar `dbt build --select dim_tempo`
{{
  config(
    materialized='table'
    , schema='gold'
    , file_format='iceberg'
    , tags=["gold", "calendario", "dimensao_conformada"]
  )
}}

WITH date_spine AS (
    SELECT explode(sequence(
        TO_DATE('{{ var("dim_tempo_start_date", "2010-01-01") }}')
      , TO_DATE('{{ var("dim_tempo_end_date",   "2030-01-01") }}')
      , INTERVAL 1 DAY
    )) AS dt
)

, years AS (
    SELECT DISTINCT YEAR(dt) AS yr FROM date_spine
)

-- ── Algoritmo de Meeus/Jones/Butcher — Páscoa por ano ────────────────────────
, easter_a AS (
    SELECT yr
         , yr % 19            AS a
         , yr / 100           AS b
         , yr % 100           AS c
    FROM years
)
, easter_b AS (
    SELECT yr, a, b, c
         , b / 4              AS d
         , b % 4              AS e
         , (b + 8) / 25       AS f
    FROM easter_a
)
, easter_c AS (
    SELECT yr, a, b, c, d, e, f
         , (b - f + 1) / 3   AS g
    FROM easter_b
)
, easter_d AS (
    SELECT yr, a, b, c, d, e, f, g
         , (19 * a + b - d - g + 15) % 30  AS h
    FROM easter_c
)
, easter_e AS (
    SELECT yr, a, b, c, d, e, f, g, h
         , c / 4              AS i
         , c % 4              AS k
    FROM easter_d
)
, easter_f AS (
    SELECT yr, a, b, c, d, e, f, g, h, i, k
         , (32 + 2 * e + 2 * i - h - k) % 7  AS l
    FROM easter_e
)
, easter_g AS (
    SELECT yr, a, b, c, d, e, f, g, h, i, k, l
         , (a + 11 * h + 22 * l) / 451  AS m
    FROM easter_f
)
, easter_dates AS (
    SELECT
        yr
      , TO_DATE(CONCAT(
            CAST(yr AS STRING), '-'
          , LPAD(CAST((h + l - 7 * m + 114) / 31       AS STRING), 2, '0'), '-'
          , LPAD(CAST(((h + l - 7 * m + 114) % 31 + 1) AS STRING), 2, '0')
        )) AS dt_pascoa
    FROM easter_g
)

-- ── Feriados nacionais brasileiros — fixos + móveis ───────────────────────────
, feriados AS (
    -- Fixos (aliases explícitos — Spark não expõe col1/col2 no UNION)
    SELECT yr, TO_DATE(CONCAT(yr, '-01-01')) AS dt_feriado, 'Confraternização Universal (Ano Novo)' AS nm_feriado FROM years
    UNION ALL SELECT yr, TO_DATE(CONCAT(yr, '-04-21')) AS dt_feriado, 'Tiradentes' AS nm_feriado                   FROM years
    UNION ALL SELECT yr, TO_DATE(CONCAT(yr, '-05-01')) AS dt_feriado, 'Dia do Trabalho' AS nm_feriado              FROM years
    UNION ALL SELECT yr, TO_DATE(CONCAT(yr, '-09-07')) AS dt_feriado, 'Independência do Brasil' AS nm_feriado      FROM years
    UNION ALL SELECT yr, TO_DATE(CONCAT(yr, '-10-12')) AS dt_feriado, 'Nossa Senhora Aparecida' AS nm_feriado      FROM years
    UNION ALL SELECT yr, TO_DATE(CONCAT(yr, '-11-02')) AS dt_feriado, 'Finados' AS nm_feriado                      FROM years
    UNION ALL SELECT yr, TO_DATE(CONCAT(yr, '-11-15')) AS dt_feriado, 'Proclamação da República' AS nm_feriado     FROM years
    UNION ALL SELECT yr, TO_DATE(CONCAT(yr, '-11-20')) AS dt_feriado, 'Dia da Consciência Negra' AS nm_feriado     FROM years
    UNION ALL SELECT yr, TO_DATE(CONCAT(yr, '-12-25')) AS dt_feriado, 'Natal' AS nm_feriado                        FROM years
    -- Móveis (calculados a partir da Páscoa via Meeus/Jones/Butcher)
    UNION ALL SELECT e.yr, DATE_ADD(e.dt_pascoa, -48) AS dt_feriado, 'Carnaval (Segunda-feira)' AS nm_feriado        FROM easter_dates e
    UNION ALL SELECT e.yr, DATE_ADD(e.dt_pascoa, -47) AS dt_feriado, 'Carnaval (Terça-feira)' AS nm_feriado          FROM easter_dates e
    UNION ALL SELECT e.yr, DATE_ADD(e.dt_pascoa,  -2) AS dt_feriado, 'Sexta-feira Santa' AS nm_feriado               FROM easter_dates e
    UNION ALL SELECT e.yr, e.dt_pascoa               AS dt_feriado, 'Páscoa' AS nm_feriado                           FROM easter_dates e
    UNION ALL SELECT e.yr, DATE_ADD(e.dt_pascoa,  60) AS dt_feriado, 'Corpus Christi' AS nm_feriado                  FROM easter_dates e
)
, feriados_dedup AS (
    -- garante unicidade caso dois feriados caiam no mesmo dia (ex.: Páscoa + Consciência Negra)
    SELECT
        dt_feriado
      , nm_feriado
      , ROW_NUMBER() OVER (PARTITION BY dt_feriado ORDER BY nm_feriado) AS rn
    FROM feriados
)

-- ── Base calendário com atributos calculados ──────────────────────────────────
, calendario_base AS (
    SELECT
        -- Surrogate key — inteiro YYYYMMDD
          CAST(DATE_FORMAT(ds.dt, 'yyyyMMdd') AS INT)              AS sk_data

        -- ── Data ────────────────────────────────────────────────────────────────
        , ds.dt                                                     AS dt_data
        , DATE_FORMAT(ds.dt, 'yyyy-MM-dd')                         AS dt_data_iso
        , DATE_FORMAT(ds.dt, 'dd/MM/yyyy')                         AS dt_data_br

        -- ── Dia ─────────────────────────────────────────────────────────────────
        , DAY(ds.dt)                                                AS nr_dia_mes
        , DAYOFYEAR(ds.dt)                                          AS nr_dia_ano
        , DAYOFWEEK(ds.dt)                                          AS nr_dia_semana   -- 1=Dom 2=Seg … 7=Sáb
        , CASE DAYOFWEEK(ds.dt)
              WHEN 1 THEN 'Domingo'        WHEN 2 THEN 'Segunda-feira'
              WHEN 3 THEN 'Terça-feira'    WHEN 4 THEN 'Quarta-feira'
              WHEN 5 THEN 'Quinta-feira'   WHEN 6 THEN 'Sexta-feira'
              WHEN 7 THEN 'Sábado'
          END                                                       AS nm_dia_semana
        , CASE DAYOFWEEK(ds.dt)
              WHEN 1 THEN 'Dom'  WHEN 2 THEN 'Seg'  WHEN 3 THEN 'Ter'
              WHEN 4 THEN 'Qua'  WHEN 5 THEN 'Qui'  WHEN 6 THEN 'Sex'
              WHEN 7 THEN 'Sáb'
          END                                                       AS nm_dia_semana_abrev
        , CASE WHEN DAYOFWEEK(ds.dt) IN (1, 7) THEN TRUE ELSE FALSE END
                                                                    AS ie_fim_semana
        , CASE WHEN f.dt_feriado IS NOT NULL THEN TRUE ELSE FALSE END
                                                                    AS ie_feriado_nacional
        , COALESCE(f.nm_feriado, '')                                AS nm_feriado
        , CASE
              WHEN DAYOFWEEK(ds.dt) NOT IN (1, 7)
               AND f.dt_feriado IS NULL
              THEN TRUE ELSE FALSE
          END                                                        AS ie_dia_util
        -- helper interno — removido no SELECT final
        , CASE
              WHEN DAYOFWEEK(ds.dt) NOT IN (1, 7)
               AND f.dt_feriado IS NULL
              THEN 1 ELSE 0
          END                                                        AS _util_flag

        -- ── Semana ──────────────────────────────────────────────────────────────
        , WEEKOFYEAR(ds.dt)                                          AS nr_semana_ano_iso
        , CONCAT(
              CAST(YEAR(ds.dt) AS STRING), '-W'
            , LPAD(CAST(WEEKOFYEAR(ds.dt) AS STRING), 2, '0')
          )                                                          AS nm_ano_semana_iso   -- '2024-W03'
        , CAST(CEIL(DAY(ds.dt) / 7.0) AS INT)                       AS nr_semana_mes
        , CAST(DATE_TRUNC('WEEK', ds.dt) AS DATE)                    AS dt_inicio_semana_seg
        , DATE_ADD(CAST(DATE_TRUNC('WEEK', ds.dt) AS DATE), 6)       AS dt_fim_semana_dom

        -- ── Mês ─────────────────────────────────────────────────────────────────
        , MONTH(ds.dt)                                               AS nr_mes
        , CASE MONTH(ds.dt)
              WHEN  1 THEN 'Janeiro'    WHEN  2 THEN 'Fevereiro'  WHEN  3 THEN 'Março'
              WHEN  4 THEN 'Abril'      WHEN  5 THEN 'Maio'       WHEN  6 THEN 'Junho'
              WHEN  7 THEN 'Julho'      WHEN  8 THEN 'Agosto'     WHEN  9 THEN 'Setembro'
              WHEN 10 THEN 'Outubro'    WHEN 11 THEN 'Novembro'   WHEN 12 THEN 'Dezembro'
          END                                                        AS nm_mes
        , CASE MONTH(ds.dt)
              WHEN  1 THEN 'Jan'  WHEN  2 THEN 'Fev'  WHEN  3 THEN 'Mar'
              WHEN  4 THEN 'Abr'  WHEN  5 THEN 'Mai'  WHEN  6 THEN 'Jun'
              WHEN  7 THEN 'Jul'  WHEN  8 THEN 'Ago'  WHEN  9 THEN 'Set'
              WHEN 10 THEN 'Out'  WHEN 11 THEN 'Nov'  WHEN 12 THEN 'Dez'
          END                                                        AS nm_mes_abrev
        , CONCAT(
              CASE MONTH(ds.dt)
                  WHEN  1 THEN 'Jan'  WHEN  2 THEN 'Fev'  WHEN  3 THEN 'Mar'
                  WHEN  4 THEN 'Abr'  WHEN  5 THEN 'Mai'  WHEN  6 THEN 'Jun'
                  WHEN  7 THEN 'Jul'  WHEN  8 THEN 'Ago'  WHEN  9 THEN 'Set'
                  WHEN 10 THEN 'Out'  WHEN 11 THEN 'Nov'  WHEN 12 THEN 'Dez'
              END, '/', CAST(YEAR(ds.dt) AS STRING)
          )                                                          AS nm_competencia      -- 'Jan/2024'
        , CAST(DATE_FORMAT(ds.dt, 'yyyyMM') AS INT)                  AS nr_ano_mes          -- 202401
        , CAST(DATE_TRUNC('MONTH', ds.dt) AS DATE)                   AS dt_inicio_mes
        , LAST_DAY(ds.dt)                                            AS dt_fim_mes
        , DATEDIFF(LAST_DAY(ds.dt), DATE_TRUNC('MONTH', ds.dt)) + 1 AS nr_dias_no_mes
        , CASE WHEN DAY(ds.dt) = 1 THEN TRUE ELSE FALSE END          AS ie_primeiro_dia_mes
        , CASE WHEN ds.dt = LAST_DAY(ds.dt) THEN TRUE ELSE FALSE END AS ie_ultimo_dia_mes

        -- ── Trimestre ───────────────────────────────────────────────────────────
        , QUARTER(ds.dt)                                             AS nr_trimestre
        , CONCAT(
              'T', CAST(QUARTER(ds.dt) AS STRING)
            , ' ', CAST(YEAR(ds.dt) AS STRING)
          )                                                          AS nm_trimestre        -- 'T1 2024'
        , CAST(DATE_TRUNC('QUARTER', ds.dt) AS DATE)                 AS dt_inicio_trimestre
        , LAST_DAY(ADD_MONTHS(DATE_TRUNC('QUARTER', ds.dt), 2))      AS dt_fim_trimestre

        -- ── Semestre ────────────────────────────────────────────────────────────
        , CASE WHEN MONTH(ds.dt) <= 6 THEN 1 ELSE 2 END             AS nr_semestre
        , CONCAT(
              'S', CAST(CASE WHEN MONTH(ds.dt) <= 6 THEN 1 ELSE 2 END AS STRING)
            , ' ', CAST(YEAR(ds.dt) AS STRING)
          )                                                          AS nm_semestre         -- 'S1 2024'
        , CASE WHEN MONTH(ds.dt) <= 6
               THEN CAST(DATE_TRUNC('YEAR', ds.dt) AS DATE)
               ELSE CAST(ADD_MONTHS(DATE_TRUNC('YEAR', ds.dt), 6) AS DATE)
          END                                                        AS dt_inicio_semestre
        , CASE WHEN MONTH(ds.dt) <= 6
               THEN LAST_DAY(ADD_MONTHS(DATE_TRUNC('YEAR', ds.dt), 5))
               ELSE LAST_DAY(ADD_MONTHS(DATE_TRUNC('YEAR', ds.dt), 11))
          END                                                        AS dt_fim_semestre

        -- ── Ano ─────────────────────────────────────────────────────────────────
        , YEAR(ds.dt)                                                AS nr_ano
        , YEAR(ds.dt)                                                AS nr_ano_fiscal  -- fiscal = calendário no Austa
        , CAST(DATE_TRUNC('YEAR', ds.dt) AS DATE)                    AS dt_inicio_ano
        , LAST_DAY(ADD_MONTHS(DATE_TRUNC('YEAR', ds.dt), 11))        AS dt_fim_ano
        , CASE
              WHEN YEAR(ds.dt) % 400 = 0
                OR (YEAR(ds.dt) % 4 = 0 AND YEAR(ds.dt) % 100 != 0)
              THEN TRUE ELSE FALSE
          END                                                        AS ie_ano_bissexto

        -- ── Relativo (precisos no momento da última execução do dbt) ────────────
        , CASE
              WHEN DATE_FORMAT(ds.dt, 'yyyyMM') = DATE_FORMAT(
                  FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo'), 'yyyyMM'
              ) THEN TRUE ELSE FALSE
          END                                                        AS ie_mes_atual
        , CASE
              WHEN YEAR(ds.dt) = YEAR(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo'))
              THEN TRUE ELSE FALSE
          END                                                        AS ie_ano_atual
        , CASE
              WHEN ds.dt < CAST(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS DATE)
              THEN TRUE ELSE FALSE
          END                                                        AS ie_data_passada
        , CASE
              WHEN ds.dt > CAST(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS DATE)
              THEN TRUE ELSE FALSE
          END                                                        AS ie_data_futura

        -- ── Páscoa do ano ────────────────────────────────────────────────────────
        , e.dt_pascoa                                                AS dt_pascoa_ano

        -- ── Auditoria ────────────────────────────────────────────────────────────
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')
                                                                     AS _gold_loaded_at

    FROM date_spine ds
    LEFT JOIN feriados_dedup f ON ds.dt = f.dt_feriado AND f.rn = 1
    LEFT JOIN easter_dates e   ON YEAR(ds.dt) = e.yr
)

-- ── Contadores de dias úteis via window functions ─────────────────────────────
, final AS (
    SELECT
          sk_data
        , dt_data
        , dt_data_iso
        , dt_data_br
        , nr_dia_mes
        , nr_dia_ano
        , nr_dia_semana
        , nm_dia_semana
        , nm_dia_semana_abrev
        , ie_fim_semana
        , ie_feriado_nacional
        , nm_feriado
        , ie_dia_util
        , CAST(SUM(_util_flag) OVER (
              PARTITION BY nr_ano_mes
              ORDER BY dt_data
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS INT)                                                   AS nr_dia_util_seq_mes
        , CAST(SUM(_util_flag) OVER (
              PARTITION BY nr_ano_mes
          ) AS INT)                                                   AS nr_total_dias_uteis_mes
        , nr_semana_ano_iso
        , nm_ano_semana_iso
        , nr_semana_mes
        , dt_inicio_semana_seg
        , dt_fim_semana_dom
        , nr_mes
        , nm_mes
        , nm_mes_abrev
        , nm_competencia
        , nr_ano_mes
        , dt_inicio_mes
        , dt_fim_mes
        , nr_dias_no_mes
        , ie_primeiro_dia_mes
        , ie_ultimo_dia_mes
        , nr_trimestre
        , nm_trimestre
        , dt_inicio_trimestre
        , dt_fim_trimestre
        , nr_semestre
        , nm_semestre
        , dt_inicio_semestre
        , dt_fim_semestre
        , nr_ano
        , nr_ano_fiscal
        , dt_inicio_ano
        , dt_fim_ano
        , ie_ano_bissexto
        , ie_mes_atual
        , ie_ano_atual
        , ie_data_passada
        , ie_data_futura
        , dt_pascoa_ano
        , _gold_loaded_at
    FROM calendario_base
)

SELECT * FROM final
