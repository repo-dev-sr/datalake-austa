"""
Configuração compartilhada para DAGs Astronomer Cosmos + dbt-spark (lakehouse Tasy).

LoadMode.DBT_LS: descoberta de nós via `dbt ls` no parse da DAG — não exige `target/manifest.json`
no scheduler (evita Broken DAG quando o artefato ainda não foi gerado no volume).

Para usar manifest pré-gerado (CI/imagem), volte a `LoadMode.DBT_MANIFEST` e defina
`ProjectConfig(manifest_path=...)` apontando para um arquivo existente.

DbtTaskGroup: use `from cosmos import DbtTaskGroup` (reexport do pacote cosmos).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict

from cosmos import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode

from common.config import DBT_PROFILE_NAME, DBT_PROJECT_DIR, DBT_PROFILES_DIR, DBT_TARGET

logger = logging.getLogger(__name__)


def _pythonpath_with_dbt_plugins() -> dict[str, str]:
    """PYTHONPATH com `dbt/plugins` — necessário no parse (`dbt ls`) e na execução."""
    project_path = Path(DBT_PROJECT_DIR).resolve()
    plugins = str(project_path / "plugins")
    prev = os.environ.get("PYTHONPATH", "")
    return {
        "PYTHONPATH": f"{plugins}{os.pathsep}{prev}" if prev else plugins,
    }


def _dbt_env_vars() -> dict[str, str]:
    """Env para parse e execução dbt (Cosmos: usar só ProjectConfig.env_vars, não operator_args['env'])."""
    merged = dict(os.environ)
    merged.update(_pythonpath_with_dbt_plugins())
    return merged


def get_project_config() -> ProjectConfig:
    project_path = Path(DBT_PROJECT_DIR).resolve()
    return ProjectConfig(
        dbt_project_path=str(project_path),
        env_vars=_dbt_env_vars(),
    )


def get_profile_config() -> ProfileConfig:
    target = DBT_TARGET
    profiles_dir = Path(DBT_PROFILES_DIR).resolve()
    profiles_yml = profiles_dir / "profiles.yml"
    profiles_example = profiles_dir / "profiles.yml.example"

    if not profiles_yml.exists() and profiles_example.exists():
        logger.warning(
            "profiles.yml não encontrado em %s; usando fallback %s",
            profiles_yml,
            profiles_example,
        )
        profiles_yml = profiles_example

    if not profiles_yml.exists():
        raise FileNotFoundError(
            "Nenhum profile dbt encontrado. Esperado: "
            f"'{profiles_dir / 'profiles.yml'}' "
            f"ou '{profiles_dir / 'profiles.yml.example'}'. "
            "Defina DBT_PROFILES_DIR corretamente ou provisione profiles.yml no deploy."
        )

    return ProfileConfig(
        profile_name=DBT_PROFILE_NAME,
        target_name=target,
        profiles_yml_filepath=str(profiles_yml),
    )


def render_config_for_select(select: list[str]) -> RenderConfig:
    return RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=select,
    )


def dbt_operator_args() -> Dict[str, Any]:
    """Argumentos dos operadores Cosmos (dbt-spark).

    Variáveis de ambiente (incl. PYTHONPATH para `dbt/plugins`) vêm de `ProjectConfig.env_vars`
    em `get_project_config()` — não use `env` aqui (mutuamente exclusivo no Cosmos).

    - **pool** `spark_dbt`: limita concorrência global de jobs dbt+Kyuubi (poucos slots).
    - **queue** `dbt`: encaminha para o worker Celery `airflow-worker-dbt` (Compose EC2).
    """
    return {
        "install_deps": True,
        "pool": "spark_dbt",
        "queue": "dbt",
    }


def layer_dbt_task_group(group_id: str, select: list[str]) -> DbtTaskGroup:
    """Um DbtTaskGroup com seleção dbt (modelo, path:, tag:, etc.)."""
    return DbtTaskGroup(
        group_id=group_id,
        project_config=get_project_config(),
        profile_config=get_profile_config(),
        render_config=render_config_for_select(select),
        operator_args=dbt_operator_args(),
    )
