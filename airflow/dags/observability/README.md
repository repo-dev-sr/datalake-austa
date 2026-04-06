# Observabilidade (Airflow)

DAGs de smoke / testes leves sobre o lakehouse.

## E-mail em falha

`OBSERVABILITY_DEFAULT_ARGS` em `common/default_args.py` usa `AIRFLOW_ALERT_EMAIL` (fallback: `rsantos@rhemadata.com`).

Para o envio funcionar, configure SMTP no Airflow, por exemplo:

- `AIRFLOW__SMTP__SMTP_HOST`
- `AIRFLOW__SMTP__SMTP_PORT`
- `AIRFLOW__SMTP__SMTP_USER` / `AIRFLOW__SMTP__SMTP_PASSWORD` (ou secret)
- `AIRFLOW__SMTP__SMTP_MAIL_FROM`
- `AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp`

Sem SMTP, as tasks ainda rodam; apenas o e-mail de alerta não será entregue.

## Pool / fila

`lakehouse_dbt_tests_smoke` usa **`pool=spark_dbt`** e **`queue=dbt`** (mesmo padrão que Cosmos). Ver `docs/AIRFLOW_OPERACAO.md` na pasta `airflow/` do repositório.
