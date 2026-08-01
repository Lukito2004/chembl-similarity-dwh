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
