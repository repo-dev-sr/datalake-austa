# Runbook CDC Bronze (estado atual)

Este runbook padroniza a execucao dos modelos Bronze com:
- estado atual por chave de negocio;
- watermark incremental em `__ts_ms` com lookback;
- soft delete (`is_deleted`);
- reprocessamento parcial e total.

## Variaveis de controle

Definidas em `dbt_project.yml`:
- `cdc_lookback_hours` (default: `2`)
- `cdc_reprocess_hours` (default: `0`)

## Execucao normal

Executa incremental com watermark da propria tabela alvo:

```bash
dbt run --select models/bronze
```

Ou por modelo:

```bash
dbt run --select bronze_tasy_atend_paciente_unidade
```

## Reprocessamento parcial

Reprocessa a janela recente (em horas), sobrepondo o watermark normal.

Exemplo 24h:

```bash
dbt run --select bronze_tasy_atend_paciente_unidade --vars '{"cdc_reprocess_hours": 24}'
```

## Recriacao da tabela (reprocessamento total)

Quando necessario recriar com os novos padroes:

```bash
dbt run --select bronze_tasy_atend_paciente_unidade --full-refresh
```

Para toda a camada Bronze:

```bash
dbt run --select models/bronze --full-refresh
```

## Validacao recomendada

1. `dbt compile --select bronze_tasy_atend_paciente_unidade`
2. `dbt test --select bronze_tasy_atend_paciente_unidade`
3. Executar um incremental normal
4. Executar reprocesso parcial (ex.: 24h) e validar idempotencia

## Observacoes de performance

- Evite janela fixa grande no filtro; use watermark com lookback curto.
- Use reprocesso parcial somente quando necessario.
- Full refresh deve ser reservado para mudancas estruturais ou reconciliacao.
- Se o raw estiver particionado por `year/month/day/hour`, combinar filtro de particao
  com o watermark reduz leitura de arquivos.
