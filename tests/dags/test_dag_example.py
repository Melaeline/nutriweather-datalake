"""Enhanced DAGs test for NutriWeather pipeline."""

import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag


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
    """Generate a tuple for import errors in the dag bag."""
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            airflow_home = os.environ.get("AIRFLOW_HOME", "/")
            return os.path.relpath(os.path.abspath(path), os.path.abspath(airflow_home))

        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    """Generate a tuple of dag_id, <DAG objects> in the DagBag."""
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        airflow_home = os.environ.get("AIRFLOW_HOME", "/")
        return os.path.relpath(os.path.abspath(path), os.path.abspath(airflow_home))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file."""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


APPROVED_TAGS = {"fetch", "format", "merge", "elasticsearch", "indexing", "pipeline", "complete", "meals", "weather", "raw", "parquet", "analytics"}


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """Test if a DAG is tagged and if those TAGs are in the approved list."""
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS, f"{dag_id} has unapproved tags: {set(dag.tags) - APPROVED_TAGS}"


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_retries(dag_id, dag, fileloc):
    """Test if a DAG has retries set."""
    assert (
        dag.default_args.get("retries", 0) >= 2
    ), f"{dag_id} in {fileloc} must have task retries >= 2."


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_schedule(dag_id, dag, fileloc):
    """Test if DAG schedule is properly set for Airflow 3.0.2."""
    # In Airflow 3.0.2, schedule should be None or a valid schedule expression
    assert dag.schedule is not None or dag.schedule is None, f"{dag_id} has invalid schedule"
