import logging

from pathlib import Path

from airflow.models import DagBag


def test_surf_data_raw_to_csv_dag_loads_and_has_expected_tasks():
    logging.getLogger("airflow").disabled = True

    repo_root = Path(__file__).resolve().parents[2]
    dags_dir = repo_root / "dags"
    dag_bag = DagBag(dag_folder=str(dags_dir), include_examples=False)

    # Airflow 3.x's DagBag.get_dag may consult the metadata DB; avoid DB access in unit tests.
    dag = dag_bag.dags.get("surf_data_raw_to_csv")
    assert dag is not None

    task_ids = [t.task_id for t in dag.tasks]
    assert set(task_ids) == {
        "poll_for_ready_batch",
        "extract_to_csv",
        "mark_raw_processed",
        "delete_old_raw_files",
    }

    # Basic dependency chain: poll -> extract -> mark -> delete
    assert dag.get_task("extract_to_csv").upstream_task_ids == {"poll_for_ready_batch"}
    assert dag.get_task("mark_raw_processed").upstream_task_ids == {"extract_to_csv"}
    assert dag.get_task("delete_old_raw_files").upstream_task_ids == {"mark_raw_processed"}
