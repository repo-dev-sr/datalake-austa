# dbt — Lakehouse TASY

Execução local (fora do Airflow) e publicação no repositório [datalake-austa](https://github.com/Dev-Infra-Grupo-AMH/datalake-austa).

## Pré-requisitos

- Python 3.10+ (recomendado)
- Acesso de rede ao host Spark Thrift (Kyuubi) e permissões no Glue/S3 conforme o ambiente

## Setup

```bash
cd dbt
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements-dbt.txt

cp .env.example .env
# Edite .env com host, usuário e senha LDAP

cp profiles.yml.example profiles.yml
# Opcional: profiles.yml só é necessário se não usar exclusivamente env vars no example (o example já usa env_var)

export DBT_PROFILES_DIR="$(pwd)"
set -a && source .env && set +a
dbt debug
```

Comandos úteis: `dbt compile`, `dbt run --select models/bronze`, `dbt test`. Detalhes de CDC Bronze: [RUNBOOK_CDC_BRONZE.md](RUNBOOK_CDC_BRONZE.md).

## Arquivos sensíveis

Não commite `profiles.yml` nem `.env` — use `profiles.yml.example` e `.env.example`. O `.gitignore` da pasta `dbt` já os ignora.

Na raiz do repositório [datalake-austa](https://github.com/Dev-Infra-Grupo-AMH/datalake-austa), o `.gitignore` inclui exceções `!.env.example` e `!**/.env.example` para que o exemplo não seja ignorado pelo padrão `.env.*`.
