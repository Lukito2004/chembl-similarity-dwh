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


def test_summarise_runs_after_the_quality_gate():
    assert dag.get_task("summarise").upstream_task_ids == {"run_quality_checks"}
    assert dag.get_task("run_quality_checks").upstream_task_ids == {"build_silver"}


def test_the_silver_dataset_publishes_only_after_the_gate():
    from chembl_datasets import SILVER_MOLECULE

    assert not dag.get_task("build_silver").outlets
    assert SILVER_MOLECULE in dag.get_task("run_quality_checks").outlets


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


def test_the_pivot_is_built_after_the_gate_clears_the_mart():
    from airflow.models import DagBag

    mart_dag = DagBag("dags", include_examples=False).dags["similarity_mart"]
    assert mart_dag.get_task("build_pivot_view").upstream_task_ids == {"run_quality_checks"}
    assert mart_dag.get_task("run_quality_checks").upstream_task_ids == {"build_mart"}


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


def test_the_source_dataset_publishes_only_after_the_gate():
    from airflow.models import DagBag

    from chembl_datasets import SOURCE_MOLECULE

    input_dag = DagBag("dags", include_examples=False).dags["input_compounds"]
    assert not input_dag.get_task("select_source_molecules").outlets
    assert SOURCE_MOLECULE in input_dag.get_task("run_quality_checks").outlets


def prune_probe(monkeypatch, live, results):
    """Run the real summarise callable with S3 stubbed, returning what it deleted."""
    import fingerprint_build_dag

    deleted = []
    monkeypatch.setattr(fingerprint_build_dag, "list_keys", lambda prefix: live)
    monkeypatch.setattr(
        fingerprint_build_dag, "fingerprint_shard_key", lambda index: f"p/part-{index:05d}.parquet"
    )
    monkeypatch.setattr(fingerprint_build_dag, "delete_keys", lambda keys: deleted.extend(keys))
    summarise = fingerprint_build_dag.dag.get_task("summarise").python_callable
    return deleted, summarise(results)


def shard_result(index):
    return {"index": index, "written": 10, "rejected": 0, "bytes": 1000}


def test_a_rebuild_of_the_same_size_prunes_nothing(monkeypatch):
    live = [f"p/part-{i:05d}.parquet" for i in range(3)]
    deleted, totals = prune_probe(monkeypatch, live, [shard_result(i) for i in range(3)])
    assert deleted == []
    assert totals["pruned"] == 0
    assert totals["shards"] == 3


def test_a_smaller_rebuild_prunes_only_the_shards_it_did_not_write(monkeypatch):
    live = [f"p/part-{i:05d}.parquet" for i in range(5)]
    deleted, totals = prune_probe(monkeypatch, live, [shard_result(i) for i in range(3)])
    assert deleted == ["p/part-00003.parquet", "p/part-00004.parquet"]
    assert totals["pruned"] == 2


def test_a_larger_rebuild_prunes_nothing(monkeypatch):
    live = [f"p/part-{i:05d}.parquet" for i in range(2)]
    deleted, totals = prune_probe(monkeypatch, live, [shard_result(i) for i in range(4)])
    assert deleted == []
    assert totals["shards"] == 4


def test_a_failed_check_fails_the_task_without_retrying(monkeypatch):
    import pytest
    from airflow.exceptions import AirflowFailException

    import similarity_mart_dag
    from chembl_sim.quality.runner import QualityGateError

    def refuse(*args, **kwargs):
        raise QualityGateError("dim_holds_no_unreferenced_molecules observed 3, expected 0")

    monkeypatch.setattr(similarity_mart_dag, "run_checks", refuse)
    monkeypatch.setattr(similarity_mart_dag, "get_current_context", lambda: {"run_id": "r"})
    gate = similarity_mart_dag.dag.get_task("run_quality_checks").python_callable
    with pytest.raises(AirflowFailException, match="unreferenced_molecules observed 3"):
        gate()


def callable_for(module, task_id):
    return module.dag.get_task(task_id).python_callable


def test_the_branch_sends_each_path_to_its_own_task(monkeypatch):
    import chembl_ingest_dag as mod

    choose = callable_for(mod, "choose_ingest_path")
    monkeypatch.setattr(mod, "get_current_context", lambda: {"params": {"ingest_path": "dump"}})
    assert choose() == "load_from_dump"
    monkeypatch.setattr(mod, "get_current_context", lambda: {"params": {"ingest_path": "api"}})
    assert choose() == "fetch_api_release"


def test_the_dump_task_passes_the_force_parameter_through(monkeypatch):
    import chembl_ingest_dag as mod

    seen = {}

    class Result:
        release = "ChEMBL_37"
        rows = 5

    def fake_ingest(force):
        seen["force"] = force
        return Result()

    monkeypatch.setattr(mod, "get_current_context", lambda: {"params": {"force_reingest": True}})
    monkeypatch.setattr(mod, "ingest_from_dump", fake_ingest)
    assert callable_for(mod, "load_from_dump")() == {"release": "ChEMBL_37", "rows": 5}
    assert seen["force"] is True


def test_each_api_task_ingests_its_own_resource(monkeypatch):
    import chembl_ingest_dag as mod

    calls = []
    monkeypatch.setattr(mod, "get_current_context", lambda: {"params": {"force_reingest": False}})
    monkeypatch.setattr(
        mod, "ingest_resource", lambda resource, release, force: calls.append(resource.name) or 7
    )
    assert callable_for(mod, "ingest_molecules")("ChEMBL_37") == 7
    assert callable_for(mod, "ingest_chembl_id_lookup")("ChEMBL_37") == 7
    assert calls == [mod.MOLECULE.name, mod.CHEMBL_ID_LOOKUP.name]


def test_the_ingest_summary_counts_every_bronze_table(fake_warehouse):
    import chembl_ingest_dag as mod

    fake_warehouse(mod, [(11,), (22,), (33,), (44,), (55,), [("molecule", "ChEMBL_37", True)]])
    counts = callable_for(mod, "summarise")()
    assert len(counts) == len(mod.BRONZE_TABLES) + 1
    assert counts["bronze.molecule_dictionary"] == 11
    assert counts["silver.molecule"] == 55


def test_plan_shards_leaves_the_last_range_open_ended(fake_warehouse):
    import fingerprint_build_dag as mod

    fake_warehouse(mod, [[("CHEMBL1",), ("CHEMBL5",), ("CHEMBL9",)]])
    shards = callable_for(mod, "plan_shards")()
    assert [s["index"] for s in shards] == [0, 1, 2]
    assert shards[0] == {"index": 0, "first_chembl_id": "CHEMBL1", "last_chembl_id": "CHEMBL5"}
    assert shards[-1]["last_chembl_id"] is None


def stub_shard_writer(mod, monkeypatch):
    from chembl_sim.chem.fingerprints import FingerprintBatch

    monkeypatch.setattr(
        mod,
        "fingerprint_rows",
        lambda rows, radius, n_bits: FingerprintBatch(["CHEMBL1"], [b"\x00" * 256], 2),
    )
    monkeypatch.setattr(mod, "fingerprint_table", lambda ids, fps: "table")
    monkeypatch.setattr(mod, "fingerprint_shard_key", lambda index: f"key-{index}")
    monkeypatch.setattr(mod, "write_parquet", lambda table, key: 4096)


def test_an_open_ended_shard_uses_the_tail_query(fake_warehouse, monkeypatch):
    import fingerprint_build_dag as mod

    connection = fake_warehouse(mod, [[("CHEMBL1", "CCO")]])
    stub_shard_writer(mod, monkeypatch)
    out = callable_for(mod, "build_shard")(
        {"index": 3, "first_chembl_id": "CHEMBL1", "last_chembl_id": None}
    )
    assert out == {"index": 3, "written": 1, "rejected": 2, "bytes": 4096}
    assert connection.cursor_object.statements[0] == mod.SHARD_TAIL
    assert connection.cursor_object.params[0] == ("CHEMBL1",)


def test_a_bounded_shard_uses_the_range_query(fake_warehouse, monkeypatch):
    import fingerprint_build_dag as mod

    connection = fake_warehouse(mod, [[("CHEMBL1", "CCO")]])
    stub_shard_writer(mod, monkeypatch)
    callable_for(mod, "build_shard")(
        {"index": 0, "first_chembl_id": "CHEMBL1", "last_chembl_id": "CHEMBL5"}
    )
    assert connection.cursor_object.statements[0] == mod.SHARD_RANGE
    assert connection.cursor_object.params[0] == ("CHEMBL1", "CHEMBL5")


def test_only_csv_objects_are_landed_into_bronze(fake_warehouse, monkeypatch):
    import input_compounds_dag as mod

    landed = []
    monkeypatch.setattr(mod, "list_keys", lambda prefix: ["a/one.csv", "a/notes.txt", "a/two.CSV"])
    monkeypatch.setattr(mod, "read_text", lambda key: "compound_name\nAspirin\n")
    monkeypatch.setattr(
        mod,
        "insert_rows",
        lambda cursor, schema, table, columns, rows: landed.append(rows) or len(rows),
    )
    fake_warehouse(mod)
    assert callable_for(mod, "load_input_files")() == 2
    assert len(landed) == 2


def test_the_source_selection_is_reported_by_origin(monkeypatch):
    import input_compounds_dag as mod

    class Selection:
        total, from_input, topped_up, rejected = 100, 56, 44, 2

    monkeypatch.setattr(mod, "build_source_molecules", lambda: Selection())
    assert callable_for(mod, "select_source_molecules")() == {
        "total": 100,
        "from_input": 56,
        "topped_up": 44,
        "rejected": 2,
    }


def test_the_search_and_mart_tasks_report_their_counts(monkeypatch):
    import similarity_mart_dag as mod

    class Search:
        sources, top_rows, flagged_rows = 100, 1000, 40

    class Load:
        dimension_rows, fact_rows = 1099, 1000

    monkeypatch.setattr(mod, "run_similarity_search", lambda: Search())
    monkeypatch.setattr(mod, "load_mart", lambda: Load())
    monkeypatch.setattr(mod, "create_pivot_view", lambda: ["CHEMBL1"])
    assert callable_for(mod, "compute_similarity")() == {
        "sources": 100,
        "top_rows": 1000,
        "flagged_rows": 40,
    }
    assert callable_for(mod, "build_mart")() == {"dimension_rows": 1099, "fact_rows": 1000}
    assert callable_for(mod, "build_pivot_view")() == ["CHEMBL1"]


def test_the_mart_summary_merges_both_inputs(fake_warehouse):
    import similarity_mart_dag as mod

    fake_warehouse(mod, [(100,)])
    totals = callable_for(mod, "summarise")(
        {"sources": 100, "top_rows": 1000, "flagged_rows": 40},
        {"dimension_rows": 1099, "fact_rows": 1000},
    )
    assert totals["distinct_sources_in_fact"] == 100
    assert totals["sources"] == 100 and totals["fact_rows"] == 1000


def test_each_gate_checks_the_layer_its_own_dag_produced(monkeypatch):
    import chembl_ingest_dag
    import input_compounds_dag
    import similarity_mart_dag

    class Report:
        def summary(self):
            return {"checks": 1, "passed": 1, "warnings": 0, "blocking": 0}

    seen = {}

    def recorder(module):
        def run_checks(run_id, layers):
            seen[module.dag.dag_id] = layers
            return Report()

        return run_checks

    for module in (chembl_ingest_dag, input_compounds_dag, similarity_mart_dag):
        monkeypatch.setattr(module, "get_current_context", lambda: {"run_id": "r"})
        monkeypatch.setattr(module, "run_checks", recorder(module))
        assert callable_for(module, "run_quality_checks")() == Report().summary()

    assert seen == {
        "chembl_ingest": ("silver",),
        "input_compounds": ("source",),
        "similarity_mart": ("gold",),
    }
