# Holdings Onboarding Lakehouse

> **Everything here is synthetic.** Keelstone, both clients (Calder Valley Teachers' Retirement System, Wrexford County Firefighters' Pension Fund) and every custodian (Harrow Trust, Linden Bank, Pemberton Custody, Holloway Custody, Quayside Trust Company) are fictional. No real bank, client or fund data is used.

![Architecture](docs/architecture.png)

A forward-deployed engineer at a fictional investment-data platform onboards a client whose holdings arrive from three custodians in three formats. Onboarding a new source should be a YAML file, not new code. Full spec: [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md).

## Status

| Slice | What | Status |
|---|---|---|
| 1 | Data in: generator with planted problems, landing volume, bronze via Auto Loader, quarantine | done |
| 2 | Silver: config-driven mapping to one position model, security master resolution, units rule, dedupe | done |
| 3 | Gold: trusted positions in USD, exposure, reconciliation with pending late files, AI/BI dashboard | done |
| 4 | Orchestration per client (file arrival), a fourth custodian and a second client onboarded by config | done |
| 5 to 8 | Scale, AWS, AI assistant, docs | not started |

## Layout

```
config/clients/<client>.yml     which sources a client has, and its Unity Catalog schema
config/sources/<source>.yml     format, delimiter, columns, quarantine rules, header aliases
src/keelstone/generator/        fixed-seed generator that plants every problem in spec section 3
src/keelstone/ingest/           generic bronze ingest (one Auto Loader stream per configured source)
src/keelstone/silver/           silver mapping (config to SQL), pipeline definitions, local mirror
src/keelstone/gold/             reconciliation rules and gold pipeline definitions
dashboards/                     AI/BI dashboard template and per-client dashboards (generated)
pipelines/                      Lakeflow declarative pipeline source (thin; logic is in the wheel)
src/keelstone/qa/               where ground truth lives; pipeline code may not import it
jobs/                           Databricks notebooks: setup, ingest, validation
resources/clients/              per-client pipeline, job and dashboard (generated from the client YAML)
scripts/                        generate, deliver (upload by delivery date), build wheel, deploy
tests/                          local pytest (plain Python, no Spark needed)
```

## Run it

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest                                        # local tests (no Spark needed)

.venv/bin/python scripts/generate.py                              # data/<client>/landing + ground_truth
databricks auth login --host <workspace-url> --profile keelstone
.venv/bin/python scripts/render_client_resources.py               # per-client bundle resources from YAML
.venv/bin/python scripts/deploy.py                                # wheel + configs to a volume, bundle deploy
.venv/bin/python scripts/deliver.py --client calder_valley --ground-truth   # ground truth to keelstone.qa only
.venv/bin/python scripts/deliver.py --client calder_valley --all  # files land; the file-arrival trigger runs the job
python scripts/run_job.py client_calder_valley idempotency_check=true      # update, re-run, full refresh, validate

# Replay a late file (slice 3): reset, deliver in two steps, the job runs on each arrival
python scripts/reset_client.py --client calder_valley --yes        # empties landing + bronze (synthetic data)
python scripts/deliver.py --client calder_valley --through 2026-08-11
python scripts/deliver.py --client calder_valley --all
```

## Design notes for slice 1

- **Bronze is config-driven.** `ingest_bronze` reads the client YAML and starts one Auto Loader stream per listed source from that source's YAML. No source-specific code.
- **Exactly once.** Auto Loader checkpoints load each file path once. A file re-sent under a new name is caught by a SHA-256 of its bytes and logged as `duplicate_content`. Every Delta write uses `txnAppId`/`txnVersion`, so a retried micro-batch is not written twice.
- **Nothing silently dropped.** Every row lands in the source's bronze table or in `bronze_quarantine` with a reason (`malformed_record`, `missing_required:<col>`, `not_numeric:<col>`, `bad_date:<col>`). Every file gets one row in `bronze_file_log`.
- **Schema drift keeps loading.** Columns are matched by header name. Unknown columns go to `_rescued_data` and are logged per file in `bronze_schema_drift_log`. A known rename is mapped by a `column_aliases` entry in the source YAML.
- **Ground truth stays out of the pipeline.** It lives locally in `data/ground_truth/` and in the `keelstone.qa` volume. A test fails if ingest (or later silver and gold) code references it. Only validation jobs read it.

## Design notes for slice 2

- **Silver is a Lakeflow declarative pipeline.** A check on Free Edition showed a serverless pipeline installs the project wheel from the volume (`environment.dependencies`), loads its source file, records expectation counts in the event log, and AUTO CDC (the pipeline form of MERGE) keeps re-runs unchanged. So silver uses one pipeline rather than plain PySpark jobs.
- **Mapping is config.** Each source YAML has a `silver:` section: which bronze column feeds each canonical field, the identifier type and columns, and the units multiplier. Dates use the format already in `rules.dates`. Pemberton's quantity is `lots[].quantity`, summed per position. Linden's renamed headers are `header_versions` in its YAML; bronze aliases are derived from them and every silver row records its `header_version`.
- **One position model.** `silver_positions`: client_id, source_id, position_side, account_id, security_id, business_date, quantity, price, market_value (local currency, after the multiplier), currency, identifier_type, identifier, header_version, source_file. Natural key: client, source, account, security, business date. The book of record maps into the same model (no price or value; gold values it).
- **Security master resolution.** `silver_identifier_map` maps ISIN, CUSIP, ticker:exchange and security_id to one security_id. An identifier with no match, or more than one, goes to `unresolved_securities` with its source and identifier, never dropped.
- **Units rule.** A row whose market value / (quantity x price) is between 500 and 2000, or between 0.0005 and 0.002, is quarantined in `silver_quarantine` with reason `units_scale_suspect`.
- **Dedupe.** Identical rows within a file are collapsed by the natural-key MERGE; `silver_dedupe_log` records how many per file. `silver_key_conflicts` lists any natural key seen with different content, and must stay empty.
- **Accounting.** For every file: bronze rows = silver + silver quarantine + unresolved + deduped. The Slice 2 job checks this, compares each part with the ground truth, and proves idempotency by snapshotting every silver table after an update, a re-run and a full refresh.

## Design notes for slice 3

- **Gold is in the same pipeline as silver**, as materialized views over the silver tables. Free Edition allows one active pipeline per type; adding datasets to the existing pipeline needs no second one. Materialized views are recomputed from silver, so re-runs leave them unchanged.
- **Trusted positions.** `gold_positions` is the client's book of record per business date, valued at closing prices, converted to USD with the FX table, with each position's reconciliation status attached.
- **Exposure.** `gold_exposure_asset_class` and `gold_exposure_currency`: market value in USD (and local currency for currencies) with weights, per client and date.
- **Reconciliation.** `gold_reconciliation` compares the book with the custodians per account, security and date. Breaks: `quantity_break`, `value_break` (beyond the larger of an absolute and a relative tolerance set in the client YAML), `missing_at_custodian`, `missing_in_book`. If a custodian's file for that date has not arrived, the book position is `pending_late_file` (status pending, not a break) until it does.
- **Nothing unexplained.** A book position whose custodian row was held back upstream is not reported as missing. It gets status `explained_upstream` with the queue that already holds it: `custodian_row_rejected` (bronze quarantine), `custodian_row_quarantined` (silver quarantine) or `custodian_identifier_unresolved` (unresolved securities). A rejected row whose key cannot be read is attributed by count within the same custodian, date and account (`explanation_basis = count`).
- **Tie-out rate** = matched / (compared minus pending), per date in `gold_tieout_daily`.
- **Measured on the slice 3 dataset (three custodians, 60 business days, seed 42; slice 4 results with Holloway Custody are below):** 33,374 positions compared; 32,687 matched; 11 breaks (4 quantity, 3 value, 2 missing at custodian, 2 missing in book, all planted); 676 explained upstream; 0 pending once every file arrived. Tie-out rate 97.94% overall, 98.78% on the last date, lowest 63.93% on 2026-08-25 (the day Linden sent values in units, quarantined as `units_scale_suspect`). Before the late Pemberton file arrived, its 187 book positions showed as pending and the tie-out rate was 98.50%.
- **Dashboard.** `dashboards/gold_overview.lvdash.json` (AI/BI) is deployed by the bundle: tie-out rate over time, exposure by asset class and currency, non-matched positions by reason, and the break queue.

## Slice 4: orchestration and onboarding by config

### Onboarding by config: the numbers

| | Holloway Custody (4th custodian for Calder Valley) | Wrexford County Firefighters' Pension Fund (2nd client) |
|---|---|---|
| **Lines of YAML** | **34**: `config/sources/holloway.yml` (new, 33) and one line in `config/clients/calder_valley.yml` | **109**: `config/clients/wrexford.yml` (18), `config/sources/linden_wrexford.yml` (31), `quayside.yml` (31), `wrexford_book.yml` (29) |
| **Engine changes needed** | **2, both generic** (see below) | **none** |
| **Delivery to validated gold** | **13.9 min** (full refresh plus the update, re-run and full-refresh idempotency check) | **12.7 min** from first file delivered to validation passed; the file-arrival run itself took 9.2 min |
| Files changed | 2 config files | 4 config files, plus 2 files generated from its YAML by `scripts/render_client_resources.py` (`resources/clients/wrexford.yml`, `dashboards/wrexford.lvdash.json`) |
| Writing the config to a clean local dry run | 3 min 14 s, including finding and fixing the 2 engine gaps | 8 s (written by Claude Code, so not representative of a person) |
| Config ready to validated gold, wall clock | 1 h 31 min. This includes about 18 min of slice 4 platform work before the first deploy and about 45 min lost to the one-time table-ownership problem below, neither caused by the new custodian | 39 min, including the same 18 min of platform work before the first deploy |

Generator and ground-truth additions for the new synthetic data (`src/keelstone/generator/clients.py`, `extra.py`) are not counted as onboarding: they create the test data, not the pipeline. Adding Holloway left every Harrow, Linden and Pemberton file byte-identical (a test checks this against the slices 1 to 3 dataset).

### What onboarding taught the engine

Two generic engine changes, both needed by Holloway Custody's file format and neither specific to any source:

1. **Any day, month and year order for dates.** The engine accepted only four date formats, and Holloway sends `dd.MM.yyyy`. Config validation now accepts any order of `yyyy`, `MM` and `dd` with one consistent separator (or none); Spark and the local mirror already parsed date patterns generically.
2. **Decimal and thousands separators.** Holloway writes `3.802.151,16`. Without this, every price failed the numeric rule, and worse, a quantity like `38.700` would have passed silently as 38.7. Any source can now set `decimal_separator` and `thousands_separator`. Numbers are normalised before the bronze numeric rule, in every silver cast (including nested tax lots) and in the local mirror; config validation rejects a separator that is also the delimiter.

Platform tooling added in this slice (needed to run many clients, not by any one format):

- **`scripts/render_client_resources.py`** builds each client's pipeline, orchestration job and AI/BI dashboard from its client YAML and one dashboard template. It is generic: it contains no client or source names and reads only the client id, catalog, schema, landing volume and display name. Onboarding a client is its YAML plus this script.
- **`jobs/wait_for_pipeline_slot.py`** runs clients' pipelines one at a time (below).
- **Delivery-aware validation.** The silver and gold validations check only files that have landed, read unresolved identifiers from a generic ground-truth list, and work out pending late files from the landing volume, so they hold for any client on any day.

### The one-time table-ownership problem, and the fix

Moving from slice 3's single `silver` pipeline to one pipeline per client meant deleting the old pipeline. Databricks keeps a deleted pipeline's tables in a restoration period and still treats them as owned by it, so the new Calder Valley pipeline was refused ("Table ... is already managed by pipeline ..."). Its pipeline task failed twice, about 15 minutes each, before the run was cancelled. Wrexford, a new schema, was not affected. The fix was the one Databricks documents: move each of the 17 silver and gold tables to the new pipeline with `ALTER STREAMING TABLE | MATERIALIZED VIEW ... SET TBLPROPERTIES ('pipelines.pipelineId' = '<new pipeline id>')`, then run the client job with `first_update=full_refresh`. It passed every check. This only happens when a pipeline is replaced, not when a client or source is onboarded; the same move is needed if the `demo` target replaces `dev`.

### Orchestration

- **One Lakeflow Job per client**, generated from its YAML: file arrival on the client's landing volume, then setup, bronze ingest, wait for a free pipeline slot, the client's own silver and gold pipeline, then bronze, silver and gold validation against the ground truth.
- **File-arrival triggers on Unity Catalog volumes work on Free Edition.** A test file landed at 21:17:23 and the triggered run started at 21:18:38; in the backfill, Wrexford's files triggered its run at 21:47:06. Two limits found: a file that lands before the trigger's first check after a deploy does not trigger a run, and overwriting an existing file with the same name does not trigger one either (only new files do).
- **Client pipelines run in sequence.** Free Edition allows one active pipeline per type (it does allow several to exist). Each client job runs `wait_for_pipeline_slot` before its pipeline task: it lists the other clients' pipelines and waits while any update is active. In the backfill, both jobs triggered within 90 seconds of each other; Calder Valley's job waited 30 seconds for Wrexford's pipeline to finish before starting its own. Pipeline tasks also retry twice, five minutes apart, in case two jobs pass the check at the same moment.
- **Backfill.** All 60 days of both clients were delivered at once (482 and 301 files); each client's trigger ran one job that ingested everything.
- **Re-running a date is idempotent.** All of Wrexford's 2026-08-20 files were delivered again under the same names (overwriting them). The next run ingested 0 files and 0 rows, and its update, re-run and full refresh left all 16 silver and gold tables identical, with every count still matching the ground truth. Calder Valley's run (full refresh, re-run, full refresh) did the same for its tables.
- **Deploy targets.** `dev` (default) prefixes every name with `[dev <user>]`. `demo` has the same resources without the prefix, for screenshots: `KEELSTONE_TARGET=demo python scripts/deploy.py`. Deploy one or the other, not both: two pipelines cannot manage the same tables.

### Results

| | Calder Valley (4 custodians) | Wrexford (2 custodians) |
|---|---|---|
| Files | 482 (481 loaded, the re-sent duplicate loaded once) | 301 |
| Bronze rows = silver + quarantine + unresolved + deduped | 97,467 = 96,744 + 193 + 524 + 6 | 39,756 = 39,649 + 0 + 103 + 4 |
| Planted breaks found with the right reason | 15 of 15 (5 quantity, 4 value, 3 missing at custodian, 3 missing in book) | 8 of 8 (2 of each kind) |
| Unexplained rows | 0 | 0 |
| Tie-out rate | 98.07% (lowest 68.08% on the Linden units-error day) | 98.88% |
| Idempotency (update, re-run, full refresh) | every silver and gold table identical | every silver and gold table identical (after re-delivering a date) |
| Lineage reads from another client's schema | none (1,003 lineage records checked) | none (115 lineage records checked; the first run found none yet, as lineage arrives with a delay) |

### Client isolation

Each client has its own schema, landing volume, checkpoint volume, pipeline and job. `tests/test_client_isolation.py` builds each client's pipeline with recording stand-ins for Spark (no Spark needed), runs every dataset definition, and asserts that every table it reads is its own pipeline dataset or a table in its own schema, and never the other client's. On Databricks, the gold validation also checks Unity Catalog lineage for reads from other schemas.

## Databricks Free Edition notes

- Serverless only, no retries in dev, small data steps, so a quota error stops the run rather than looping.

### Workspace-file limitation and the wheel-in-a-volume workaround

**Limitation.** On this Free Edition workspace, serverless compute could not reliably read bundle files under `/Workspace`. A `spark_python_task` script, and in one run an `import` of a module from the bundle's `src/`, failed with `OSError: [Errno 5]` (a 403 from workspace storage). The same files read fine through the API, and retries failed the same way, so it is a platform issue rather than our code. Notebook tasks load fine, and Unity Catalog volumes read and write reliably.

**Workaround.** `scripts/deploy.py` builds a wheel of `src/keelstone` (standard library only) and uploads it, with `config/`, to the volume `keelstone.platform.artifacts`, then runs `databricks bundle deploy`. Every job task is a notebook whose first cell is `%pip install` of that wheel, and it reads YAML from the volume. Onboarding a new source is still config only: edit YAML, run `deploy.py`.
