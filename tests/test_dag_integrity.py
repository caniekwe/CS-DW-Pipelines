"""Integrity checks for every DAG in dags/.

These run inside the project's own Airflow image so that the dependency set under
test is the same one the scheduler uses in production. To run them locally:

    docker build -t cs-dw-pipelines:ci .
    docker run --rm --entrypoint bash \
      -v "$PWD/dags:/opt/airflow/dags:ro" \
      -v "$PWD/tests:/opt/airflow/tests:ro" \
      cs-dw-pipelines:ci \
      -c "pip install -q pytest && pytest /opt/airflow/tests -v"

The suite deliberately checks structure only. It never connects to the staging or
warehouse databases, so it is safe to run anywhere.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from airflow.models.dagbag import DagBag

DAG_FOLDER = Path(os.environ.get("DAG_FOLDER", "/opt/airflow/dags"))

# Modules that live in dags/ but are not expected to define a DAG (shared helpers,
# for example). Every other .py file must produce at least one DAG.
NON_DAG_MODULES: set[str] = set()

# DAGs that predate the standard failure-logging callback. Remove an entry once the
# DAG sets on_failure_callback in its default_args - test_failure_callback_allowlist_is_current
# fails if an entry here is no longer needed, so the list cannot go stale.
DAGS_WITHOUT_FAILURE_CALLBACK = {"hr_pipeline"}

DAGBAG = DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)

ALL_DAGS = sorted(DAGBAG.dags.items())
DAG_IDS = [dag_id for dag_id, _ in ALL_DAGS]


def _dag_params():
    """Parametrise over every parsed DAG, keeping the dag_id as the test id."""
    return pytest.mark.parametrize("dag", [dag for _, dag in ALL_DAGS], ids=DAG_IDS)


def test_dag_folder_is_populated():
    """Guard against a bad mount silently turning every other test into a no-op."""
    assert DAG_FOLDER.is_dir(), f"DAG folder does not exist: {DAG_FOLDER}"
    py_files = [p for p in DAG_FOLDER.glob("*.py") if not p.name.startswith("_")]
    assert py_files, f"No Python files found in {DAG_FOLDER}"


def test_no_import_errors():
    """Every file in dags/ must import cleanly. This is the check that matters most."""
    if not DAGBAG.import_errors:
        return

    report = "\n\n".join(
        f"--- {filename} ---\n{stacktrace}"
        for filename, stacktrace in sorted(DAGBAG.import_errors.items())
    )
    pytest.fail(
        f"{len(DAGBAG.import_errors)} DAG file(s) failed to import:\n\n{report}",
        pytrace=False,
    )


def test_dags_were_discovered():
    assert DAGBAG.dags, (
        f"No DAGs were discovered in {DAG_FOLDER}. Either the folder is wrong or "
        "every file failed to import."
    )


def test_every_module_defines_a_dag():
    """Catch a DAG file that parses but never registers a DAG.

    These pipelines are built with the @dag decorator and only become a DAG when the
    factory is called at the bottom of the module, which is easy to drop.
    """
    defining_files = {Path(dag.fileloc).name for dag in DAGBAG.dags.values()}
    candidates = {
        p.name
        for p in DAG_FOLDER.glob("*.py")
        if not p.name.startswith("_") and p.stem not in NON_DAG_MODULES
    }
    missing = sorted(candidates - defining_files)
    assert not missing, (
        "These files are in dags/ but define no DAG - did you forget to call the "
        f"DAG factory at the end of the module? {missing}"
    )


@_dag_params()
def test_dag_has_tags(dag):
    """Tags are how the team filters the DAG list in the Airflow UI."""
    assert dag.tags, f"{dag.dag_id} has no tags"


@_dag_params()
def test_dag_has_catchup_disabled(dag):
    """These DAGs run hourly against a 'latest upload' query.

    Catchup would queue one run per missed interval and reprocess the same file
    repeatedly, so it must stay off.
    """
    assert dag.catchup is False, f"{dag.dag_id} has catchup enabled"


@_dag_params()
def test_dag_has_failure_callback(dag):
    """Task failures must be recorded in etl_task_failures via log_task_failure."""
    if dag.dag_id in DAGS_WITHOUT_FAILURE_CALLBACK:
        pytest.skip(f"{dag.dag_id} is a known gap - see DAGS_WITHOUT_FAILURE_CALLBACK")

    assert dag.default_args.get("on_failure_callback") is not None, (
        f"{dag.dag_id} does not set on_failure_callback in default_args, so task "
        "failures will not be written to etl_task_failures"
    )


def test_failure_callback_allowlist_is_current():
    """Stop DAGS_WITHOUT_FAILURE_CALLBACK from outliving the gaps it documents."""
    fixed = sorted(
        dag_id
        for dag_id in DAGS_WITHOUT_FAILURE_CALLBACK
        if dag_id in DAGBAG.dags
        and DAGBAG.dags[dag_id].default_args.get("on_failure_callback") is not None
    )
    assert not fixed, (
        "These DAGs now set on_failure_callback and should be removed from "
        f"DAGS_WITHOUT_FAILURE_CALLBACK: {fixed}"
    )

    unknown = sorted(DAGS_WITHOUT_FAILURE_CALLBACK - set(DAGBAG.dags))
    assert not unknown, (
        f"DAGS_WITHOUT_FAILURE_CALLBACK names DAGs that no longer exist: {unknown}"
    )


@_dag_params()
def test_dag_has_no_cycles(dag):
    # Airflow 3 exposes this on the DAG itself; the helper module is deprecated.
    if hasattr(dag, "check_cycle"):
        dag.check_cycle()
        return

    try:  # pragma: no cover - older Airflow only
        from airflow.utils.dag_cycle_tester import check_cycle
    except ImportError:  # pragma: no cover
        pytest.skip("check_cycle is not available in this Airflow version")

    check_cycle(dag)
