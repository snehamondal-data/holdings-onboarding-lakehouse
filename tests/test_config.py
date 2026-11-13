import shutil
from pathlib import Path

import pytest
import yaml

from keelstone.config import ConfigError, load_client, parse_client, parse_source, plan_ingest

REPO = Path(__file__).resolve().parents[1]
CONFIG = REPO / "config"


def test_every_client_and_source_config_is_valid():
    for path in (CONFIG / "clients").glob("*.yml"):
        plans = plan_ingest(path.stem)
        assert plans, path


def test_plan_has_one_stream_per_listed_source_with_isolated_paths():
    client = load_client("calder_valley")
    plans = plan_ingest("calder_valley")
    assert [p.source.source_id for p in plans] == list(client.sources)
    assert len({p.checkpoint_path for p in plans}) == len(plans)
    assert len({p.bronze_table for p in plans}) == len(plans)
    for p in plans:
        assert p.input_path.startswith("/Volumes/keelstone/calder_valley/landing/")
        assert p.checkpoint_path.startswith("/Volumes/keelstone/calder_valley/_checkpoints/")
        assert p.bronze_table.startswith("keelstone.calder_valley.")
        assert p.reader_options["cloudFiles.schemaEvolutionMode"] == "rescue"


def test_generator_landing_layout_matches_source_configs(any_client):
    client_id, _, truth = any_client
    plans = {p.source.source_id: p for p in plan_ingest(client_id)}
    sources_with_files = {f["source"] for f in truth["files"].values()}
    assert sources_with_files == set(plans)
    for rel, f in truth["files"].items():
        assert rel.startswith(plans[f["source"]].source.landing_subdir + "/"), rel


def test_new_source_added_only_in_yaml_is_picked_up(tmp_path):
    """Onboarding by config: a fourth custodian is two YAML edits, no code."""
    cfg = tmp_path / "config"
    shutil.copytree(CONFIG, cfg)
    (cfg / "sources" / "fernhill.yml").write_text(yaml.safe_dump({
        "source_id": "fernhill", "display_name": "Fernhill Bank (fictional, test only)",
        "kind": "custodian", "format": "csv", "landing_subdir": "fernhill", "file_glob": "*.tsv",
        "options": {"delimiter": "\t", "header": True}, "bronze_table": "bronze_fernhill",
        "columns": ["acct", "sedol", "units", "val_date"],
        "rules": {"required": ["acct", "sedol"], "numeric": ["units"],
                  "dates": {"val_date": "yyyyMMdd"}},
    }))
    client_path = cfg / "clients" / "calder_valley.yml"
    client = yaml.safe_load(client_path.read_text())
    client["sources"].append("fernhill")
    client_path.write_text(yaml.safe_dump(client))

    plans = plan_ingest("calder_valley", config_dir=cfg)
    new = [p for p in plans if p.source.source_id == "fernhill"]
    assert len(new) == 1
    p = new[0]
    assert p.input_path == "/Volumes/keelstone/calder_valley/landing/fernhill"
    assert p.bronze_table == "keelstone.calder_valley.bronze_fernhill"
    assert p.reader_options["sep"] == "\t"
    assert p.reader_options["pathGlobFilter"] == "*.tsv"
    assert "`sedol` string" in p.source.schema_ddl()


def test_second_client_gets_its_own_schema(tmp_path):
    cfg = tmp_path / "config"
    shutil.copytree(CONFIG, cfg)
    (cfg / "clients" / "other_fund.yml").write_text(yaml.safe_dump({
        "client_id": "other_fund", "catalog": "keelstone", "schema": "other_fund",
        "sources": ["harrow"]}))
    a = {p.bronze_table for p in plan_ingest("calder_valley", config_dir=cfg)}
    b = {p.bronze_table for p in plan_ingest("other_fund", config_dir=cfg)}
    assert a.isdisjoint(b)
    assert all(t.startswith("keelstone.other_fund.") for t in b)


@pytest.mark.parametrize("bad,msg", [
    ({"format": "xml"}, "format"),
    ({"columns": ["a", "a"]}, "duplicate"),
    ({"rules": {"required": ["nope"]}}, "required column"),
    ({"rules": {"dates": {"a": "dd-MMM-yy"}}}, "date format"),
    ({"landing_subdir": "../other_client"}, "landing_subdir"),
    ({"options": {}}, "delimiter"),
])
def test_bad_source_config_is_rejected(bad, msg):
    base = {"source_id": "x", "kind": "custodian", "format": "csv", "landing_subdir": "x",
            "bronze_table": "bronze_x", "columns": ["a", "b"], "options": {"delimiter": ","}}
    base.update(bad)
    with pytest.raises(ConfigError, match=msg):
        parse_source(base)


def test_client_cannot_use_the_qa_schema():
    with pytest.raises(ConfigError, match="reserved"):
        parse_client({"client_id": "x", "catalog": "keelstone", "schema": "qa", "sources": []})
