"""Example DAGs test. This test ensures that all Dags have tags, retries set to two, and no import errors. This is an example pytest and may not be fit the context of your DAGs. Feel free to add and remove tests."""

import os
import logging
from contextlib import contextmanager
from pathlib import Path
import pytest
from airflow.models import DagBag


def _repo_dags_dir() -> Path:
    # tests/dags/test_dag_example.py -> repo_root/dags
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "dags"


def _strip_path_prefix(path: str) -> str:
    # Prefer making paths relative to this repo; fall back to AIRFLOW_HOME.
    dags_dir = _repo_dags_dir()
    try:
        return os.path.relpath(path, str(dags_dir.parent))
    except Exception:
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME") or os.getcwd())


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(dag_folder=str(_repo_dags_dir()), include_examples=False)

        # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
        return [(None, None)] + [
            (_strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(dag_folder=str(_repo_dags_dir()), include_examples=False)

    return [(k, v, _strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


APPROVED_TAGS = {}


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a DAG is tagged and if those TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


@pytest.mark.parametrize(
    "dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_retries(dag_id, dag, fileloc):
    """
    test if a DAG has retries set
    """
    assert (
        dag.default_args.get("retries", None) >= 2
    ), f"{dag_id} in {fileloc} must have task retries >= 2."
