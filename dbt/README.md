# dbt — Lakehouse TASY

Projeto [dbt](https://www.getdbt.com/) com **Spark (Thrift/Kyuubi)**, **Glue** e tabelas **Iceberg**. Pode ser executado **no seu laptop ou desktop** (fora do Airflow), desde que haja rede até o cluster Spark e permissões adequadas.

Repositório: [datalake-austa](https://github.com/Dev-Infra-Grupo-AMH/datalake-austa) — no clone, este guia corresponde à pasta **`dbt/`** na raiz do repo.

---

## O que você precisa

| Requisito | Detalhe |
|-----------|---------|
| Python | 3.10 ou superior (recomendado) |
| Rede | Acesso ao host **Spark Thrift** (Kyuubi), em geral via **VPN** ou rede corporativa |
| Credenciais | Usuário/senha **LDAP** do ambiente (mesmos usados no Thrift) |
| AWS | Leitura em **S3** e metadados no **Glue** costumam ser feitos pelo cluster Spark; o dbt não precisa de AWS CLI instalado só para compilar, mas o engine precisa enxergar o datalake |

---

## 1. Clonar o repositório

```bash
git clone https://github.com/Dev-Infra-Grupo-AMH/datalake-austa.git
cd datalake-austa/dbt
```

> Se você já estiver na pasta `dbt/` do clone, os próximos comandos são os mesmos.

---

## 2. Ambiente virtual Python (`.venv`)

Evita conflito com outros projetos e fixa as versões do `requirements-dbt.txt`.

**Linux / macOS:**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-dbt.txt
```

**Windows (PowerShell):**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements-dbt.txt
```

Para desativar o venv depois: `deactivate`.

---

## 3. Credenciais locais (não vão para o Git)

1. Copie os exemplos:

   ```bash
   cp .env.example .env
   cp profiles.yml.example profiles.yml
   ```

2. Edite **`.env`** e preencha:

   - `SPARK_THRIFT_HOST` — hostname do Thrift/Kyuubi  
   - `SPARK_THRIFT_PORT` — em geral `10009` (se omitir, o profile usa o default)  
   - `SPARK_THRIFT_USER` / `SPARK_THRIFT_PASSWORD` — LDAP  

3. **`profiles.yml`** pode ficar igual ao example: ele só referencia variáveis de ambiente (`env_var`). Ajuste só se o time tiver outro catálogo Spark.

Arquivos **`.env`** e **`profiles.yml`** estão no `.gitignore` — não commite.

---

## 4. Apontar o dbt para este diretório

O dbt precisa achar o `profiles.yml` **nesta pasta** (onde está o `dbt_project.yml`).

**Linux / macOS:**

```bash
cd /caminho/completo/datalake-austa/dbt
export DBT_PROFILES_DIR="$(pwd)"
set -a && source .env && set +a
```

**Windows (PowerShell)** — ajuste o caminho:

```powershell
cd C:\caminho\datalake-austa\dbt
$env:DBT_PROFILES_DIR = (Get-Location).Path
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
    Set-Item -Path "env:$($matches[1].Trim())" -Value $matches[2].Trim()
  }
}
```

> Dica: no Linux/macOS você pode colocar o `export DBT_PROFILES_DIR` e o `source .env` num script `local.sh` só na sua máquina (e ignorá-lo no Git).

---

## 5. Validar a conexão

```bash
dbt debug
```

Se falhar, confira VPN, host/porta, usuário/senha e se `DBT_PROFILES_DIR` aponta para a pasta **`dbt/`** do clone.

---

## 6. Comandos do dia a dia

| Objetivo | Comando |
|----------|---------|
| Compilar SQL sem executar | `dbt compile` |
| Rodar todos os modelos | `dbt run` |
| Só Bronze | `dbt run --select models/bronze` |
| Um modelo | `dbt run --select bronze_tasy_atend_paciente_unidade` |
| Testes | `dbt test` |
| Documentação (opcional) | `dbt docs generate` |

CDC Bronze (watermark, full-refresh, reprocessamento): **[RUNBOOK_CDC_BRONZE.md](RUNBOOK_CDC_BRONZE.md)**.

---

## 7. E o Airflow / EC2?

O deploy para o servidor (pastas `/opt/airflow/dags` e `/opt/airflow/dbt`) é feito pelo **GitHub Actions** ao dar push na branch **`main`**. O que roda na EC2 é cópia do repositório; **desenvolvimento interativo** com `dbt run` / `dbt test` costuma ser mais prático **no seu ambiente local** seguindo este README.

---

## 8. Problemas comuns

| Sintoma | O que verificar |
|---------|------------------|
| `Could not find profile named 'lakehouse_tasy'` | `DBT_PROFILES_DIR` não aponta para a pasta onde está `profiles.yml` |
| `env_var` não definida | Carregar `.env` antes do `dbt` (`set -a` / `source .env`) |
| Timeout / connection refused | VPN, security group, host/porta do Thrift |
| Erro ao ler S3/Avro | Permissões no cluster Spark/Glue, não no laptop em si |

---

## Variáveis do projeto

Parâmetros de bucket, prefixos de raw e CDC estão em **`dbt_project.yml`** (`vars`). Para sobrescrever em um comando:

```bash
dbt run --select bronze_tasy_atend_paciente_unidade --vars '{"cdc_reprocess_hours": 24}'
```

---

## Documentação adicional

- [RUNBOOK_CDC_BRONZE.md](RUNBOOK_CDC_BRONZE.md) — execução e reprocessamento da camada Bronze  
- Repositório: [datalake-austa](https://github.com/Dev-Infra-Grupo-AMH/datalake-austa)
