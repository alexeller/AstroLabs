import logging
from pathlib import Path

from airflow.models import DagBag


def test_surf_data_to_s3_dag_loads_and_has_expected_tasks_and_dependencies():
    logging.getLogger("airflow").disabled = True

    repo_root = Path(__file__).resolve().parents[2]
    dags_dir = repo_root / "dags"
    dag_bag = DagBag(dag_folder=str(dags_dir), include_examples=False)

    dag = dag_bag.dags.get("surf_data_to_s3")
    assert dag is not None

    assert set(dag.task_ids) == {
        "build_run_date",
        "fork",
        "surf_raw__tiverton_ri",
        "surf_raw__barrington_beach_ri",
        "surf_raw__easton_beach_newport_ri",
        "surf_raw__narragansett_town_beach_ri",
        "join",
        "validate_published",
    }

    build = dag.get_task("build_run_date")
    fork = dag.get_task("fork")
    join = dag.get_task("join")
    validate = dag.get_task("validate_published")

    surf_tasks = {
        "surf_raw__tiverton_ri",
        "surf_raw__barrington_beach_ri",
        "surf_raw__easton_beach_newport_ri",
        "surf_raw__narragansett_town_beach_ri",
    }

    # Explicit chain from the DAG definition.
    assert fork.task_id in build.downstream_task_ids
    assert build.task_id in fork.upstream_task_ids

    assert fork.downstream_task_ids == surf_tasks
    assert join.upstream_task_ids == surf_tasks

    assert join.task_id in validate.upstream_task_ids
    assert validate.task_id in join.downstream_task_ids
