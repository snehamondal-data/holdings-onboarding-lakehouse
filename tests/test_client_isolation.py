"""Client isolation: one client's pipeline and tables never read another client's schema.

The silver+gold pipeline is built for each client with recording stand-ins for Spark and the
pipeline API (no Spark needed). Every dataset function is executed and every table it reads,
by name or inside SQL, is collected. Each read must be either a dataset of this client's own
pipeline (unqualified, so it resolves to the pipeline's target schema) or a table qualified with
this client's own catalog.schema. The bronze ingest plans are checked the same way.
On Databricks, validate_slice3 adds a Unity Catalog lineage check of the real runs.
"""
import re
import sys
import types


import pytest

from keelstone.config import load_client, plan_ingest

CLIENTS = ("calder_valley", "wrexford")


class _Anything:
    """Absorbs every DataFrame / Column / functions call, including Column operators."""

    def __init__(self, *_a, **_k):
        pass

    def __getattr__(self, _name):
        return _Anything()

    def __call__(self, *_a, **_k):
        return _Anything()

    def __getitem__(self, _k):
        return _Anything()

    def _same(self, *_a, **_k):
        return _Anything()

    __gt__ = __lt__ = __ge__ = __le__ = __eq__ = __ne__ = _same
    __and__ = __or__ = __rand__ = __ror__ = __invert__ = __add__ = __sub__ = __mul__ = __truediv__ = _same
    __radd__ = __rsub__ = __rmul__ = __rtruediv__ = __neg__ = _same
    __hash__ = object.__hash__


class RecordingSpark:
    def __init__(self):
        self.reads: list[str] = []
        outer = self

        class _Reader:
            def table(self, name):
                outer.reads.append(name)
                return _Anything()

            def __getattr__(self, item):          # format(), option(), ... chain back to the reader
                return lambda *a, **k: self

        self.read = _Reader()
        self.readStream = _Reader()
        self.conf = _Anything()

    def sql(self, query: str):
        ctes = set(re.findall(r"(?:\bWITH|,)\s*(\w+)\s+AS\s*\(", query, flags=re.I))
        for name in re.findall(r"\b(?:FROM|JOIN)\s+([`\w.]+)", query, flags=re.I):
            if name.strip("`") not in ctes:
                self.reads.append(name.strip("`"))
        return _Anything()


class RecordingDP:
    """Stand-in for the Lakeflow pipelines module: records datasets and calls their functions later."""

    def __init__(self):
        self.functions: list = []
        self.datasets: set[str] = set()
        self.cdc_sources: list[str] = []

    def _register(self, name):
        self.datasets.add(name)

        def deco(fn):
            self.functions.append(fn)
            return fn
        return deco

    def table(self, name=None, **_):
        return self._register(name)

    def view(self, name=None, **_):
        return self._register(name)

    def materialized_view(self, name=None, **_):
        return self._register(name)

    def expect(self, *_a, **_k):
        return lambda fn: fn

    def create_streaming_table(self, name, **_):
        self.datasets.add(name)

    def create_auto_cdc_flow(self, target, source, **_):
        self.cdc_sources.append(source)


@pytest.fixture()
def fake_pyspark(monkeypatch):
    """Minimal pyspark modules so the pipeline code imports without Spark."""
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = _Anything(name="functions")
    sql.functions = functions
    sql.Window = _Anything(name="Window")
    pyspark.sql = sql
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
    yield


def _pipeline_reads(client_id: str):
    from keelstone.silver.pipeline import define
    dp, spark = RecordingDP(), RecordingSpark()
    define(dp, spark, client_id, config_dir=str(__import__("keelstone.config").config.DEFAULT_CONFIG_DIR))
    for fn in dp.functions:
        fn()
    return dp, spark.reads


@pytest.mark.parametrize("client_id", CLIENTS)
def test_pipeline_reads_only_its_own_datasets_and_schema(fake_pyspark, client_id):
    c = load_client(client_id)
    dp, reads = _pipeline_reads(client_id)
    assert reads, "the recorder saw no reads; the test would prove nothing"
    own_prefix = f"{c.catalog}.{c.schema}."
    for name in reads:
        if "." in name:
            assert name.startswith(own_prefix), f"{client_id} pipeline reads {name}"
        else:
            assert name in dp.datasets, f"{client_id} pipeline reads unknown dataset {name}"
    assert set(dp.cdc_sources) <= dp.datasets
    # and it does read its own bronze tables, so the check is not vacuous
    assert any(n.startswith(own_prefix + "bronze_") for n in reads)


@pytest.mark.parametrize("client_id,other", [("calder_valley", "wrexford"), ("wrexford", "calder_valley")])
def test_pipeline_never_names_the_other_clients_schema(fake_pyspark, client_id, other):
    o = load_client(other)
    _, reads = _pipeline_reads(client_id)
    assert not [n for n in reads if f".{o.schema}." in f".{n}" or n.startswith(f"{o.catalog}.{o.schema}")]


@pytest.mark.parametrize("client_id", CLIENTS)
def test_ingest_plans_stay_in_the_clients_schema_and_volumes(client_id):
    c = load_client(client_id)
    for p in plan_ingest(client_id):
        for table in (p.bronze_table, p.quarantine_table, p.file_log_table, p.drift_log_table):
            assert table.startswith(f"{c.catalog}.{c.schema}."), table
        for path in (p.input_path, p.checkpoint_path, p.schema_location):
            assert path.startswith(f"/Volumes/{c.catalog}/{c.schema}/"), path


def test_clients_have_distinct_schemas_and_landing_volumes():
    a, b = (load_client(c) for c in CLIENTS)
    assert a.schema != b.schema and a.landing_root != b.landing_root and a.checkpoint_root != b.checkpoint_root
