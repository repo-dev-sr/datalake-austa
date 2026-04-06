"""
Configuração compartilhada para DAGs Astronomer Cosmos + dbt-spark (lakehouse Tasy).

LoadMode.DBT_MANIFEST: lê target/manifest.json pré-compilado (requer `dbt compile` no deploy).
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

from common.config import DBT_PROJECT_DIR, DBT_PROFILES_DIR

logger = logging.getLogger(__name__)


def get_project_config() -> ProjectConfig:
    project_path = Path(DBT_PROJECT_DIR).resolve()
    return ProjectConfig(
        dbt_project_path=str(project_path),
        manifest_path=str(project_path / "target" / "manifest.json"),
    )


def get_profile_config() -> ProfileConfig:
    target = os.environ.get("DBT_TARGET", "dev")
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
        profile_name="lakehouse_tasy",
        target_name=target,
        profiles_yml_filepath=str(profiles_yml),
    )


def render_config_for_select(select: list[str]) -> RenderConfig:
    return RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        select=select,
    )


def dbt_operator_args() -> Dict[str, Any]:
    """Argumentos dos operadores Cosmos (dbt-spark).

    Env com PYTHONPATH para `dbt/plugins` (sitecustomize / perfil).

    - **pool** `spark_dbt`: limita concorrência global de jobs dbt+Kyuubi (poucos slots).
    - **queue** `dbt`: encaminha para o worker Celery `airflow-worker-dbt` (Compose EC2).
    """
    merged = os.environ.copy()
    plugins = str(Path(DBT_PROJECT_DIR).resolve() / "plugins")
    prev = merged.get("PYTHONPATH", "")
    merged["PYTHONPATH"] = f"{plugins}{os.pathsep}{prev}" if prev else plugins
    return {
        "install_deps": True,
        "pool": "spark_dbt",
        "queue": "dbt",
        "env": merged,
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
