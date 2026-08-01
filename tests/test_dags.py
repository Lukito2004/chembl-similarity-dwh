from chembl_ingest_dag import INGEST_PATH_TASKS, dag


def test_every_path_maps_to_a_real_task():
    assert set(INGEST_PATH_TASKS) == {"dump", "api"}
    for task_id in INGEST_PATH_TASKS.values():
        assert dag.has_task(task_id)


def test_the_dump_is_the_default_path():
    assert dag.params["ingest_path"] == "dump"
    assert dag.params["force_reingest"] is False


def test_only_the_two_known_paths_are_accepted():
    assert dag.params.get_param("ingest_path").schema["enum"] == ["dump", "api"]


def test_molecules_are_ingested_before_the_lookup():
    assert "ingest_chembl_id_lookup" in dag.get_task("ingest_molecules").downstream_task_ids


def test_ddl_runs_before_the_branch():
    assert "choose_ingest_path" in dag.get_task("apply_ddl").downstream_task_ids


def test_silver_is_built_after_either_branch():
    assert dag.get_task("build_silver").trigger_rule == "none_failed_min_one_success"
    upstream = dag.get_task("build_silver").upstream_task_ids
    assert upstream == {"load_from_dump", "ingest_chembl_id_lookup"}


def test_summarise_runs_after_silver():
    assert dag.get_task("summarise").upstream_task_ids == {"build_silver"}


def test_the_ingest_publishes_the_silver_dataset():
    from chembl_datasets import SILVER_MOLECULE

    outlets = dag.get_task("build_silver").outlets
    assert SILVER_MOLECULE in outlets


def test_the_fingerprint_dag_consumes_the_silver_dataset():
    from airflow.models import DagBag

    from chembl_datasets import SILVER_MOLECULE

    fingerprint_dag = DagBag("dags", include_examples=False).dags["fingerprint_build"]
    triggers = list(fingerprint_dag.timetable.dataset_condition.objects)
    assert triggers == [SILVER_MOLECULE]


def test_the_input_dag_runs_ddl_then_load_then_selection():
    from airflow.models import DagBag

    input_dag = DagBag("dags", include_examples=False).dags["input_compounds"]
    assert input_dag.get_task("load_input_files").upstream_task_ids == {"apply_ddl"}
    assert input_dag.get_task("select_source_molecules").upstream_task_ids == {"load_input_files"}


def test_the_mart_dag_waits_for_both_inputs():
    from airflow.models import DagBag

    from chembl_datasets import FINGERPRINTS, SOURCE_MOLECULE

    mart_dag = DagBag("dags", include_examples=False).dags["similarity_mart"]
    triggers = set(mart_dag.timetable.dataset_condition.objects)
    assert triggers == {FINGERPRINTS, SOURCE_MOLECULE}


def test_the_fingerprint_dag_publishes_only_when_complete():
    from airflow.models import DagBag

    from chembl_datasets import FINGERPRINTS

    fingerprint_dag = DagBag("dags", include_examples=False).dags["fingerprint_build"]
    assert FINGERPRINTS in fingerprint_dag.get_task("summarise").outlets
    assert not fingerprint_dag.get_task("build_shard").outlets


def test_the_mart_is_built_after_the_search():
    from airflow.models import DagBag

    mart_dag = DagBag("dags", include_examples=False).dags["similarity_mart"]
    assert mart_dag.get_task("build_mart").upstream_task_ids == {"compute_similarity"}


def test_the_pivot_is_built_after_the_mart():
    from airflow.models import DagBag

    mart_dag = DagBag("dags", include_examples=False).dags["similarity_mart"]
    assert mart_dag.get_task("build_pivot_view").upstream_task_ids == {"build_mart"}


def test_every_dag_alerts_on_failure():
    from airflow.models import DagBag

    from chembl_sim.alerting import notify_failure

    bag = DagBag("dags", include_examples=False)
    assert not bag.import_errors
    for dag_id in ("chembl_ingest", "fingerprint_build", "input_compounds", "similarity_mart"):
        for task in bag.dags[dag_id].tasks:
            callbacks = task.on_failure_callback
            if not isinstance(callbacks, list | tuple):
                callbacks = [callbacks]
            assert notify_failure in callbacks, f"{dag_id}.{task.task_id}"
