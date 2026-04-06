# Engineer Gold — Documentação Completa
**Lakehouse Austa · Hospital Austa · Kimball Constellation Schema**

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Suporte Multiplataforma — Windows e Linux](#2-suporte-multiplataforma--windows-e-linux)
3. [Como Invocar o Agente](#3-como-invocar-o-agente)
4. [Comandos Disponíveis](#4-comandos-disponíveis)
5. [Protocolo de Execução](#5-protocolo-de-execução)
6. [Arquitetura Gold — Constellation Schema](#6-arquitetura-gold--constellation-schema)
7. [Dimensões Conformadas](#7-dimensões-conformadas)
8. [Regras de Modelagem — Tabela Fato](#8-regras-de-modelagem--tabela-fato)
9. [Regras de Modelagem — Dimensões](#9-regras-de-modelagem--dimensões)
10. [Restrição de Fonte — Silver-Context](#10-restrição-de-fonte--silver-context)
11. [Nomenclatura](#11-nomenclatura)
12. [Templates de Código](#12-templates-de-código)
13. [Templates de Documentação (schema.yml)](#13-templates-de-documentação-schemayml)
14. [Git Workflow](#14-git-workflow)
15. [Checklist de Compliance Kimball](#15-checklist-de-compliance-kimball)
16. [Restrições Absolutas](#16-restrições-absolutas)

---

## 1. Visão Geral

O **Engineer Gold** é o agente de IA especializado na camada Gold do lakehouse hospitalar do Hospital Austa. Ele modela cubos dimensionais seguindo a metodologia **Kimball — Constellation Schema**, onde múltiplas tabelas fato compartilham dimensões conformadas.

### Stack de execução

| Componente       | Detalhe                                        |
|------------------|------------------------------------------------|
| Engine SQL       | Apache Kyuubi (sobre Spark) — **read-only**   |
| Formato tabela   | Apache Iceberg                                 |
| Catálogo         | AWS Glue (`database: gold`)                    |
| Transformação    | dbt Core                                       |
| Região AWS       | `sa-east-1`                                    |
| Orquestração     | Apache Airflow (Docker na EC2)                 |
| Source system    | Oracle Tasy (ERP hospitalar)                   |

### Posição no pipeline

```
Oracle Tasy
  └─► Debezium CDC → Kafka → S3 Raw
        └─► dbt Bronze  (CDC Avro → Iceberg)
              └─► dbt Silver  (deduplicação)
                    └─► dbt Silver-Context  (joins, grain correto)
                              └─► ★ dbt Gold  (Kimball — este agente)
```

> O Engineer Gold consome **exclusivamente** de Silver-Context. Nunca de Silver ou Bronze.

---

## 2. Suporte Multiplataforma — Windows e Linux

O agente roda localmente na máquina do usuário e detecta automaticamente o sistema operacional no Boot Check. Todos os comandos de ambiente, inventário de arquivos e instalação do AWS CLI têm variantes para **Windows** e **Linux/macOS**.

### Tabela de equivalências

| Operação | Linux / macOS | Windows (PowerShell) |
|----------|--------------|----------------------|
| Ler variável de ambiente | `${AWS_PROFILE:-default}` | `$env:AWS_PROFILE` |
| Definir variável de sessão | `export AWS_PROFILE=austa` | `$env:AWS_PROFILE = "austa"` |
| Definir variável de sessão (CMD) | — | `set AWS_PROFILE=austa` |
| Tornar variável permanente | `~/.bashrc` ou `~/.zshrc` | Variáveis de ambiente do sistema |
| Listar arquivos `.sql` | `find ... -name "*.sql"` | `Get-ChildItem -Filter *.sql` |
| Verificar arquivo existente | `ls arquivo 2>/dev/null` | `Test-Path arquivo` |
| Credenciais AWS | `~/.aws/credentials` | `%USERPROFILE%\.aws\credentials` |

### Instalação do AWS CLI

Se o AWS CLI não estiver instalado, o agente pergunta se deve instalar e, se confirmado, executa o procedimento correto para o SO detectado:

| SO | Método de instalação |
|----|----------------------|
| **Windows** | MSI silencioso via PowerShell (`AWSCLIV2.msi`) |
| **Linux (Debian/Ubuntu)** | ZIP oficial AWS via `curl` + `sudo ./install` |
| **Linux (Amazon Linux / RHEL)** | ZIP oficial AWS via `curl` + `sudo ./install` |

Após a instalação, o agente guia a criação do profile com `aws configure --profile austa` e instrui como definir `AWS_PROFILE` permanentemente em cada plataforma.

### Fluxo de decisão do Boot Check

```
Iniciar agente
  └─► Detectar SO (uname / powershell)
        └─► aws --version
              ├─► Instalado → prosseguir
              └─► Ausente
                    └─► Perguntar ao usuário
                          ├─► Aceita → instalar + criar profile → prosseguir
                          └─► Recusa → inventário local dbt apenas (sem Glue)
```

---

## 3. Como Invocar o Agente

```
/engenheiro-gold <argumento>
```

Ao ser invocado **sem argumento**, o agente exibe a mensagem de boas-vindas, executa o Boot Check e aguarda instrução.

Ao ser invocado **com argumento**, executa o protocolo completo para a tarefa informada.

---

## 4. Comandos Disponíveis

| Comando | O que faz |
|---------|-----------|
| `/engenheiro-gold cria cubo de <processo>` | Executa o protocolo completo: inventário → grain → dimensões → fato → schema.yml → branch |
| `/engenheiro-gold inventário gold` | Lista todos os fatos e dimensões existentes (dbt local + Glue Catalog) |
| `/engenheiro-gold verifica dimensões conformadas existentes` | Inspeciona `/dimensions/shared/` e o database `gold` no Glue |
| `/engenheiro-gold verifica grain de <entidade>` | Lê Silver-Context e declara o grain proposto antes de gerar qualquer SQL |
| `/engenheiro-gold adiciona métrica <métrica> em <fato>` | Estende modelo fato existente sem quebrar surrogate key ou grain |

### Exemplos de invocação

```bash
/engenheiro-gold cria cubo de produtividade médica
/engenheiro-gold cria cubo de internação hospitalar
/engenheiro-gold cria cubo de movimentação de paciente
/engenheiro-gold inventário gold
/engenheiro-gold verifica dimensões conformadas existentes
/engenheiro-gold adiciona métrica vl_glosa em fct_producao_medica
```

---

## 5. Protocolo de Execução

O agente **nunca** gera SQL sem passar pelas etapas abaixo, nessa ordem:

### Etapa 1 — Boot Check (multiplataforma: Windows e Linux)

O boot check detecta automaticamente o sistema operacional e adapta os comandos.
A sequência completa é:

#### Passo 0 — Detectar SO e verificar AWS CLI

```bash
uname -s 2>/dev/null && echo "SO: Linux/macOS" || echo "SO: Windows"
aws --version 2>/dev/null || echo "AWS_CLI_AUSENTE"
```

**Se AWS CLI não estiver instalado**, o agente apresenta:

```
⚠️  AWS CLI não encontrado nesta máquina.
    O Engineer Gold precisa do AWS CLI para consultar o Glue Catalog.

    Deseja que eu instale o AWS CLI agora? (sim/não)
```

Se o usuário confirmar, o agente instala conforme o SO detectado:

**Windows (PowerShell)**
```powershell
Invoke-WebRequest -Uri "https://awscli.amazonaws.com/AWSCLIV2.msi" -OutFile "$env:TEMP\AWSCLIV2.msi"
Start-Process msiexec.exe -Wait -ArgumentList "/i $env:TEMP\AWSCLIV2.msi /quiet /norestart"
$env:PATH = [System.Environment]::GetEnvironmentVariable("PATH","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("PATH","User")
aws --version
```

**Linux (Debian/Ubuntu/Amazon Linux/RHEL)**
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip /tmp/awscliv2.zip -d /tmp/awscli-install
sudo /tmp/awscli-install/aws/install
aws --version
```

Após instalar, o agente guia a criação do profile:

```
AWS CLI instalado! Agora configure o profile do projeto Austa:

  aws configure --profile austa

Informe quando solicitado:
  → AWS Access Key ID      (obtido no console IAM)
  → AWS Secret Access Key
  → Default region name    → sa-east-1
  → Default output format  → json

Depois defina a variável de ambiente:
  Windows (PowerShell): $env:AWS_PROFILE = "austa"
  Windows (CMD):        set AWS_PROFILE=austa
  Linux/macOS:          export AWS_PROFILE=austa

Para tornar permanente:
  Windows → Adicione AWS_PROFILE=austa nas variáveis de ambiente do sistema
  Linux   → Adicione export AWS_PROFILE=austa ao ~/.bashrc ou ~/.zshrc
```

Se o usuário recusar a instalação, o agente prossegue apenas com inventário local dbt (sem consultas ao Glue).

---

#### Passo 1 — Validar variáveis de ambiente

**Linux / macOS**
```bash
echo "AWS_PROFILE: ${AWS_PROFILE:-'⚠️  NÃO DEFINIDO — usando profile default'}"
echo "AWS_REGION:  ${AWS_REGION:-'⚠️  NÃO DEFINIDO — assumindo sa-east-1'}"
export AWS_REGION="${AWS_REGION:-sa-east-1}"
```

**Windows (PowerShell)**
```powershell
$p = if ($env:AWS_PROFILE) { $env:AWS_PROFILE } else { "⚠️  NÃO DEFINIDO — usando profile default" }
$r = if ($env:AWS_REGION)  { $env:AWS_REGION  } else { "⚠️  NÃO DEFINIDO — assumindo sa-east-1"   }
Write-Host "AWS_PROFILE: $p"
Write-Host "AWS_REGION:  $r"
if (-not $env:AWS_REGION) { $env:AWS_REGION = "sa-east-1" }
```

#### Passo 2 — Verificar entidades Silver-Context (Glue)

```bash
aws glue get-tables \
  --database-name silver_context \
  --profile "${AWS_PROFILE:-default}" \
  --region "${AWS_REGION:-sa-east-1}" \
  --query 'TableList[].Name' \
  --output table 2>/dev/null \
  || echo "⚠️  Não foi possível listar silver_context — verificar credenciais"
```

#### Passo 3 — Inventário local dbt

**Linux / macOS**
```bash
ls dbt/models/silver_context/
find dbt/models/gold/facts/ -name "*.sql" 2>/dev/null | sort || echo "Nenhum fato ainda"
find dbt/models/gold/dimensions/shared/ -name "*.sql" 2>/dev/null | sort || echo "Nenhuma dimensão ainda"
```

**Windows (PowerShell)**
```powershell
Get-ChildItem dbt/models/silver_context/ -Filter *.sql | Select-Object Name
Get-ChildItem dbt/models/gold/facts/ -Filter *.sql -ErrorAction SilentlyContinue | Select-Object Name
Get-ChildItem dbt/models/gold/dimensions/shared/ -Filter *.sql -ErrorAction SilentlyContinue | Select-Object Name
```

#### Passo 4 — Tabelas Gold no Glue

```bash
aws glue get-tables \
  --database-name gold \
  --profile "${AWS_PROFILE:-default}" \
  --region "${AWS_REGION:-sa-east-1}" \
  --query 'TableList[].{Nome: Name, Colunas: StorageDescriptor.Columns[*].Name}' \
  --output json 2>/dev/null \
  || echo "Database gold ainda não existe no Glue — Gold em estágio inicial"
```

**Erros tratados:**

| Situação | Comportamento |
|----------|---------------|
| AWS CLI ausente | Pergunta se instala; guia instalação + criação de profile |
| `AWS_PROFILE` não definido | Usa `default`; se falhar, instrui configuração (Windows e Linux) |
| Entidade ausente em Silver-Context | Bloqueia e instrui criação prévia |
| Usuário recusa instalação AWS CLI | Prossegue com inventário local dbt apenas |

---

### Etapa 2 — Relatório de Inventário Gold

Antes de qualquer SQL, apresenta ao usuário:

```
=== INVENTÁRIO GOLD ATUAL ===

FATOS EXISTENTES:
  fct_{nome}
    → Grain: {declarado no comentário linha 1 do modelo}
    → Dimensões: sk_paciente, sk_medico, ...
    → Métricas: {lista}
    → Status Glue: materializada | apenas dbt | não existe

DIMENSÕES CONFORMADAS EXISTENTES:
  dim_{entidade} → SCD{tipo} | usada por: {fatos}

OPORTUNIDADES DE REUSO PARA "{cubo solicitado}":
  ✅ REUTILIZAR: dim_{x} — será referenciada via {{ ref('dim_{x}') }}
  🆕 CRIAR:      dim_{y} — será gerada como SCD{tipo}
  ⚠️  ATENÇÃO:   fct_{z} já cobre parte deste grain — verificar sobreposição

PLANO PROPOSTO:
  1. Reutilizar: {modelos existentes}
  2. Criar novos: {modelos novos com SCD e grain}
  3. Grain do novo cubo: {declaração explícita}
  4. Fonte Silver-Context: {entidade(s)}
```

**O agente aguarda confirmação do usuário antes de avançar.**

---

### Etapa 3 — Declaração de Grain

Antes de qualquer SQL, o grain é declarado explicitamente:

```
Grain: 1 linha = 1 {evento} por {dimensão_a} / {dimensão_b} / {dimensão_c}
Exemplo: 1 linha = 1 procedimento realizado por médico/paciente/dia
```

- Se o grain for **ambíguo** → o agente pergunta ao usuário
- Se o grain **coincidir** com fato existente → o agente propõe extensão do modelo existente em vez de criar novo

---

### Etapa 4 — Checklist de Reuso de Dimensões

Para cada dimensão necessária, o agente verifica na ordem:

```
1. Existe em dbt/models/gold/dimensions/shared/?
   SIM → referenciar via {{ ref('dim_{entidade}') }} — não gera novo arquivo
   NÃO → verificar Glue Catalog (database: gold)
         SIM no Glue → criar modelo dbt correspondente ao schema existente
         NÃO no Glue → criar nova dimensão com SCD correto
```

---

### Etapa 5 — Geração de SQL (ordem obrigatória)

1. Novas dimensões (apenas as que não existem)
2. Tabela fato (referenciando `sk_` das dimensões existentes + novas)
3. `schema.yml` de cada modelo novo
4. Atualização do `schema.yml` geral da pasta `gold/`

---

### Etapa 6 — Branch e Commit

```bash
git checkout -b feat/gold-{nome-do-cubo}
git add dbt/models/gold/
git commit -m "feat(gold): add {nome} dimensional model — grain: {grain declarado}"
```

---

## 6. Arquitetura Gold — Constellation Schema

O Constellation Schema é uma extensão do Star Schema onde **múltiplas tabelas fato compartilham dimensões conformadas**. Dimensão conformada significa: mesmo conteúdo, mesmo grain, mesma surrogate key em todos os fatos que a utilizam.

```
                    ┌─────────────┐
                    │  dim_tempo  │
                    └──────┬──────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
   ┌──────┴──────┐  ┌──────┴──────┐  ┌──────┴──────┐
   │fct_producao │  │fct_internacao│  │fct_moviment.│
   │   _medica   │  │             │  │  _paciente  │
   └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
          │                │                │
    ┌─────┴──────┐   ┌─────┴─────┐   ┌──────┴─────┐
    │ dim_medico │   │dim_unidade│   │dim_paciente│
    └────────────┘   └───────────┘   └────────────┘
          │
    ┌─────┴──────────┐
    │ dim_procedimento│
    └─────────────────┘
          │
    ┌─────┴──────┐
    │ dim_convenio│
    └─────────────┘
```

### Localização dos modelos

```
dbt/models/gold/
  facts/                  → fct_{processo}.sql
  dimensions/
    shared/               → dim_{entidade}.sql  (sempre conformadas)
  schema.yml
```

---

## 7. Dimensões Conformadas

Estas dimensões são compartilhadas entre **todos** os fatos quando aplicável. Nunca recriar — sempre referenciar via `{{ ref('dim_{entidade}') }}`.

| Dimensão           | SCD | PK Natural              | Compartilhável com                        |
|--------------------|-----|-------------------------|-------------------------------------------|
| `dim_paciente`     | 2   | `cd_pessoa_fisica`      | Todos os cubos clínicos                   |
| `dim_medico`       | 2   | `cd_medico`             | Produção médica, internação, procedimento |
| `dim_convenio`     | 2   | `cd_convenio`           | Financeiro, procedimento, faturamento     |
| `dim_unidade`      | 1   | `cd_setor_atendimento`  | Internação, movimentação                  |
| `dim_tempo`        | 1   | `data (YYYYMMDD)`       | Todos os fatos                            |
| `dim_procedimento` | 1   | `cd_procedimento`       | Produção médica, faturamento              |

---

## 8. Regras de Modelagem — Tabela Fato

### Obrigatório

- **Grain declarado como comentário na linha 1** do arquivo `.sql`
- SELECT final contém apenas: FKs surrogate (`sk_`) + métricas numéricas + colunas de auditoria
- Surrogate key da fato gerada via `dbt_utils.generate_surrogate_key()`
- Natural keys preservadas com prefixo `nk_`
- Filtro `WHERE _is_deleted = FALSE` (soft deletes do Silver-Context)
- `_gold_loaded_at` via `FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')`
- Referências apenas a Silver-Context via `{{ ref('{entidade}') }}`

### Chave de tempo

```sql
CAST(DATE_FORMAT({col_data}, 'yyyyMMdd') AS INT) AS sk_data
```

Formato inteiro YYYYMMDD — compatível com `dim_tempo.sk_data`.

### Tipos de métricas

| Tipo | Descrição | Exemplo | Restrição |
|------|-----------|---------|-----------|
| **Aditiva** | Pode ser somada em qualquer dimensão | `vl_procedimento`, `qt_procedimentos` | Nenhuma |
| **Semi-aditiva** | Válida apenas em algumas dimensões | `nr_minuto_duracao`, `saldo` | Documentar `⚠️` no `schema.yml` |
| **Não-aditiva** | Nunca somar | Percentuais, médias | Documentar; não incluir sem justificativa |

### Proibido em tabelas fato

- Atributos descritivos (textos, nomes, descrições) — pertencem às dimensões
- Flags de status como atributos livres — use surrogate key para dimensão de status
- `SELECT *` no modelo final
- Referências diretas a Silver ou Bronze

---

## 9. Regras de Modelagem — Dimensões

### SCD Type 1 — entidades estáveis

Usar para: `dim_unidade`, `dim_tempo`, `dim_procedimento`, leito, CID, tipo de convênio.

```sql
-- Materialização: table (sobrescreve completo ao mudar)
-- Surrogate key: dbt_utils.generate_surrogate_key(['{pk_col}'])
-- Natural key: preservada com prefixo nk_
-- Filtro: WHERE _is_deleted = FALSE
```

### SCD Type 2 — entidades que mudam (preserva histórico)

Usar para: `dim_paciente`, `dim_medico`, `dim_convenio`.

```sql
-- Materialização: incremental, unique_key: sk_{dim}
-- Colunas obrigatórias:
--   _valid_from  → timestamp início da vigência
--   _valid_to    → timestamp fim (NULL = registro atual)
--   _is_current  → boolean (TRUE = versão vigente)
-- Surrogate key: dbt_utils.generate_surrogate_key(['{pk_col}', '_silver_processed_at'])
```

### Obrigatório em todas as dimensões

- Surrogate key via `dbt_utils.generate_surrogate_key()`
- Natural key com prefixo `nk_`
- Localização em `models/gold/dimensions/shared/`
- `_gold_loaded_at` via `FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')`

### Proibido

- Recriar dimensão que já existe em `/dimensions/shared/` — sempre referenciar
- Surrogate key manual (hash ou concatenação)
- Métricas ou cálculos em dimensões — pertencem aos fatos

---

## 10. Restrição de Fonte — Silver-Context

Gold **SEMPRE** consome de Silver-Context. Nunca de Silver ou Bronze diretamente.

```sql
-- CORRETO
SELECT * FROM {{ ref('procedimento') }}         -- silver_context.procedimento
SELECT * FROM {{ ref('atendimento') }}           -- silver_context.atendimento

-- PROIBIDO
SELECT * FROM {{ ref('silver_tasy_procedimento') }}
SELECT * FROM {{ ref('bronze_tasy_procedimento') }}
SELECT * FROM {{ source('silver', 'tasy_procedimento') }}
```

### Protocolo quando entidade não existe em Silver-Context

```
1. PARAR a geração do modelo Gold
2. Reportar ao usuário qual entidade está faltando
3. Instruir: execute /engenheiro cria silver-context para {entidade}
4. Nunca usar Silver diretamente como substituto
```

---

## 11. Nomenclatura

### Modelos SQL

| Prefixo    | Tipo               | Exemplo                          |
|------------|--------------------|----------------------------------|
| `fct_`     | Tabela fato        | `fct_producao_medica`            |
| `dim_`     | Dimensão           | `dim_paciente`, `dim_medico`     |
| `bridge_`  | Tabela ponte (M:N) | `bridge_atendimento_procedimento`|

### Colunas

| Prefixo | Tipo                   | Exemplo                              |
|---------|------------------------|--------------------------------------|
| `sk_`   | Surrogate key          | `sk_paciente`, `sk_medico`, `sk_fato`|
| `nk_`   | Natural / business key | `nk_paciente`, `nk_medico`           |
| `_`     | Metadados de auditoria | `_gold_loaded_at`, `_valid_from`     |

### Schemas no Glue / Spark

| Camada         | Schema no Glue   |
|----------------|------------------|
| Gold           | `gold`           |
| Silver-Context | `silver_context` |
| Silver         | `silver`         |
| Bronze         | `bronze`         |

### Git

| Elemento       | Padrão                                                                            |
|----------------|-----------------------------------------------------------------------------------|
| Branch         | `feat/gold-{nome-do-cubo}` — ex.: `feat/gold-producao-medica`                    |
| Commit message | `feat(gold): add fct_{nome} — grain: 1 linha por {dimensões}`                    |
| Arquivos dbt   | `{prefixo}_{nome}.sql` — documentação em `schema.yml` por pasta (não por modelo) |

---

## 12. Templates de Código

### Tabela Fato

```sql
-- Grain: 1 linha = 1 {evento} por {dimensão_a} / {dimensão_b} / {dimensão_c}
-- Fonte: silver_context.{entidade} via {{ ref('{entidade}') }}
-- Camada: Gold | Processo: {Processo} | Constellation Austa
-- Dimensões: dim_tempo · dim_{a} · dim_{b}
{{
  config(
    materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_{fato}'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "{processo}"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('{entidade}') }}
    {% if is_incremental() %}
    WHERE _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
)

, final AS (
    SELECT
        -- Surrogate key da fato
          {{ dbt_utils.generate_surrogate_key(['{pk_col}']) }}                      AS sk_{fato}

        -- Foreign keys para dimensões
        , CAST(DATE_FORMAT({col_data}, 'yyyyMMdd') AS INT)                          AS sk_data
        , {{ dbt_utils.generate_surrogate_key(['{fk_medico}']) }}                   AS sk_medico
        , {{ dbt_utils.generate_surrogate_key(['{fk_paciente}']) }}                 AS sk_paciente

        -- Natural keys preservadas
        , {pk_col}       AS nk_{entidade}
        , {fk_medico}    AS nk_medico
        , {fk_paciente}  AS nk_paciente

        -- Métricas aditivas
        , {metrica_1}
        , {metrica_2}

        -- Auditoria
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')              AS _gold_loaded_at

    FROM source
    WHERE _is_deleted = FALSE
)

SELECT * FROM final
```

---

### Dimensão SCD Type 1

```sql
-- Dimensão estável (Type 1) — sobrescreve ao mudar
-- Fonte: silver_context.{entidade}
-- Camada: Gold | Tipo: SCD1 | Dimensão conformada
{{
  config(
    materialized='table'
    , schema='gold'
    , file_format='iceberg'
    , tags=["gold", "tasy", "{entidade}", "dimensao_conformada"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('{entidade}') }}
)

, final AS (
    SELECT
          {{ dbt_utils.generate_surrogate_key(['{pk_col}']) }}    AS sk_{dim}
        , {pk_col}                                                AS nk_{dim}
        , {atributo_1}
        , {atributo_2}
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS _gold_loaded_at
    FROM source
    WHERE _is_deleted = FALSE
)

SELECT * FROM final
```

---

### Dimensão SCD Type 2

```sql
-- Dimensão que muda (Type 2) — preserva histórico completo
-- Fonte: silver_context.{entidade}
-- Camada: Gold | Tipo: SCD2 | Dimensão conformada
{{
  config(
    materialized='incremental'
    , schema='gold'
    , file_format='iceberg'
    , unique_key='sk_{dim}'
    , on_schema_change='sync_all_columns'
    , tags=["gold", "tasy", "{entidade}", "dimensao_conformada"]
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('{entidade}') }}
    {% if is_incremental() %}
    WHERE _context_processed_at > (SELECT MAX(_gold_loaded_at) FROM {{ this }})
    {% endif %}
)

, final AS (
    SELECT
          {{ dbt_utils.generate_surrogate_key(['{pk_col}', '_context_processed_at']) }} AS sk_{dim}
        , {pk_col}                                                AS nk_{dim}
        , {atributo_1}
        , {atributo_2}
        , _context_processed_at                                   AS _valid_from
        , LEAD(_context_processed_at) OVER (
              PARTITION BY {pk_col}
              ORDER BY _context_processed_at
          )                                                       AS _valid_to
        , CASE
              WHEN LEAD(_context_processed_at) OVER (
                  PARTITION BY {pk_col}
                  ORDER BY _context_processed_at
              ) IS NULL THEN TRUE
              ELSE FALSE
          END                                                     AS _is_current
        , FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS _gold_loaded_at
    FROM source
    WHERE _is_deleted = FALSE
)

SELECT * FROM final
```

---

## 13. Templates de Documentação (schema.yml)

### schema.yml — Tabela Fato

```yaml
version: 2

models:
  - name: fct_{nome}
    description: >
      {Descrição do processo de negócio}.
      Grain: 1 linha = 1 {evento} por {dimensões}.
      Fonte: Silver-Context {entidade}.
      Constellation: compartilha dim_paciente, dim_medico, dim_tempo.
    config:
      materialized: incremental
      unique_key: sk_{fato}
      on_schema_change: sync_all_columns
    tags: ["gold", "tasy", "{processo}"]
    columns:
      - name: sk_{fato}
        description: "Surrogate key da fato — gerada via dbt_utils.generate_surrogate_key"
        tests:
          - unique
          - not_null
      - name: sk_data
        description: "FK para dim_tempo — inteiro formato YYYYMMDD"
        tests:
          - not_null
      - name: sk_medico
        description: "FK para dim_medico"
        tests:
          - not_null
          - relationships:
              to: ref('dim_medico')
              field: sk_medico
      - name: sk_paciente
        description: "FK para dim_paciente"
        tests:
          - not_null
          - relationships:
              to: ref('dim_paciente')
              field: sk_paciente
      - name: nk_{entidade}
        description: "Natural key — {pk_col} do sistema Tasy"
        tests:
          - not_null
      - name: {metrica_semi_aditiva}
        description: >
          ⚠️ Semi-aditiva: somar apenas dentro do mesmo {contexto}.
          Ao agregar por {dimensão} use SUM com atenção a sobreposições.
      - name: _gold_loaded_at
        description: "Timestamp de carga na camada Gold (America/Sao_Paulo)"
```

---

### schema.yml — Dimensão

```yaml
version: 2

models:
  - name: dim_{entidade}
    description: >
      Dimensão {Entidade} — SCD Type {1|2}.
      Fonte: Silver-Context {entidade}.
      Compartilhada entre: fct_{a}, fct_{b}.
    tags: ["gold", "tasy", "{entidade}", "dimensao_conformada"]
    columns:
      - name: sk_{dim}
        description: "Surrogate key — gerada via dbt_utils.generate_surrogate_key"
        tests:
          - unique
          - not_null
      - name: nk_{dim}
        description: "Natural key — {pk_col} do sistema Tasy"
        tests:
          - not_null
      # SCD2 apenas — remover se SCD1:
      - name: _valid_from
        description: "Início da vigência do registro"
        tests:
          - not_null
      - name: _valid_to
        description: "Fim da vigência (NULL = registro atual)"
      - name: _is_current
        description: "TRUE se esta é a versão vigente do registro"
        tests:
          - not_null
      - name: _gold_loaded_at
        description: "Timestamp de carga na camada Gold (America/Sao_Paulo)"
```

---

## 14. Git Workflow

```bash
# 1. Criar branch
git checkout -b feat/gold-{nome-do-cubo}

# 2. Adicionar modelos gerados
git add dbt/models/gold/

# 3. Commit com mensagem padronizada
git commit -m "feat(gold): add fct_{nome} — grain: 1 linha por {dimensões}"

# Exemplos de mensagens válidas:
# feat(gold): add fct_producao_medica — grain: 1 linha por procedimento/médico/dia
# feat(gold): add dim_unidade (SCD1) — dimensão conformada compartilhada
# feat(gold): add dim_medico (SCD2) — migração de SCD1 provisório
```

### Regras absolutas de Git

| Permitido | Proibido |
|-----------|----------|
| Criar branch `feat/gold-*` | Commitar em `main` ou `master` |
| Abrir PR para revisão | Push sem aprovação humana |
| `git commit` com mensagem padronizada | `--no-verify` ou `--force-push` |

---

## 15. Checklist de Compliance Kimball

Aplique este checklist a cada cubo antes de abrir o PR:

### Inventário e Reuso

- [ ] Inventário Gold executado antes de qualquer SQL
- [ ] Relatório de Inventário apresentado e confirmado pelo usuário
- [ ] Dimensões existentes reutilizadas via `{{ ref('dim_{x}') }}` — não recriadas
- [ ] Grain do novo cubo não duplica grain de fato existente

### Fonte de Dados

- [ ] Todas as referências são `{{ ref('{entidade}') }}` — Silver-Context
- [ ] Nenhuma referência a `silver_tasy_*`, `bronze_tasy_*` ou `source('silver', ...)`
- [ ] Entidade consumida existe e está disponível em `silver_context`

### Tabela Fato

- [ ] Grain declarado como comentário na **linha 1** do arquivo `.sql`
- [ ] SELECT final: apenas FKs surrogate + métricas + auditoria
- [ ] Nenhum atributo descritivo na tabela fato
- [ ] Surrogate key via `dbt_utils.generate_surrogate_key()`
- [ ] Natural keys com prefixo `nk_`
- [ ] Métricas semi-aditivas documentadas com `⚠️` no `schema.yml`
- [ ] Filtro `WHERE _is_deleted = FALSE` aplicado
- [ ] `sk_data` em formato inteiro YYYYMMDD
- [ ] `_gold_loaded_at` via `FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')`

### Dimensão

- [ ] SCD Type (1 ou 2) declarado no comentário do modelo
- [ ] Surrogate key via `dbt_utils.generate_surrogate_key()`
- [ ] Natural key com prefixo `nk_`
- [ ] **SCD2**: colunas `_valid_from`, `_valid_to`, `_is_current` presentes
- [ ] **SCD2**: `LEAD()` com `PARTITION BY {pk_col} ORDER BY _context_processed_at`
- [ ] Criada em `/dimensions/shared/` (não em `/facts/`)

### schema.yml

- [ ] Grain documentado na `description` da tabela fato
- [ ] Tests `unique` + `not_null` na surrogate key de cada modelo
- [ ] Tests `relationships` nas FKs do fato → dimensões corretas
- [ ] Tags: `["gold", "tasy", "{processo}"]`
- [ ] Métricas semi-aditivas com aviso na `description`

### Código

- [ ] Snake case em tudo
- [ ] Sem `SELECT *` no modelo final
- [ ] Vírgulas à esquerda (estilo dbt)
- [ ] CTEs nomeadas semanticamente (`source`, `final`)
- [ ] Nenhuma credencial hardcoded

### Git e Deploy

- [ ] Branch com prefixo `feat/gold-`
- [ ] Commit message: `feat(gold): add {nome} — grain: {grain}`
- [ ] Nenhum commit direto em `main`
- [ ] PR aberto para revisão antes do merge
- [ ] `dbt compile` validado localmente antes do merge

---

## 16. Restrições Absolutas

| Regra | Permitido | Proibido |
|-------|-----------|----------|
| Fonte | `{{ ref('{entidade}') }}` (silver_context) | `{{ ref('silver_tasy_{x}') }}`, Bronze, Raw |
| Branch | `feat/gold-*` | `main`, `master` |
| Grain | Declarado na linha 1 do SQL | Ausente |
| Fato — conteúdo | FKs surrogate + métricas + auditoria | Atributos descritivos |
| Dimensão conformada | Referenciar existente | Recriar dimensão duplicada |
| Timestamp | `FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'America/Sao_Paulo')` | `NOW()`, `CURRENT_TIMESTAMP()` sem timezone |
| Surrogate key | `dbt_utils.generate_surrogate_key()` | Hash manual, concatenação |
| Push | Após aprovação humana via PR | Push direto em main |
| SELECT final | Colunas explícitas | `SELECT *` |
| Grain duplicado | Estender fato existente | Criar dois fatos com mesmo grain |
| Kyuubi | Read-only | DDL, DML de escrita via CLI |

---

*Documentação gerada em: 2026-04-06*
*Agente: Engineer Gold — Lakehouse Austa*
*Repositório: datalake-austa | Branch principal: main*
