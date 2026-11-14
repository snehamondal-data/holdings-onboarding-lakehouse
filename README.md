# Holdings Onboarding Lakehouse

> **Everything here is synthetic.** Keelstone, both clients (Calder Valley Teachers' Retirement System, Wrexford County Firefighters' Pension Fund) and every custodian (Harrow Trust, Linden Bank, Pemberton Custody, Holloway Custody, Quayside Trust Company) are fictional. No real bank, client or fund data is used.

![Architecture](docs/architecture.png)

## In one minute

This project shows how a forward-deployed engineer at a fictional investment-data platform could bring a new client's holdings in from several custodian banks, each sending files in its own format. Files land in a Databricks lakehouse, are loaded exactly once, mapped into one standard position model, and reconciled against the client's own records, with every difference given a reason. Onboarding is configuration, not code: a fourth custodian took 34 lines of YAML and a second client 109, each reaching checked results within 14 minutes of its files arriving. Every run is checked against planted problems with known answers, so no row is lost or left unexplained, and running it again changes nothing. It was measured at up to 15 million rows, where one change made loading 2.1 times faster and another that did not help was dropped. Files can also arrive through an AWS landing zone that checks each file and forwards it, with results ready under 12 minutes after upload. An AI assistant drafts a new source's configuration from one sample file, tests it, and waits for a person to approve it; on eight known sources it got 127 of 128 fields right for 18 cents. All data and companies are synthetic.

## Results, slice by slice

| Slice | What was built | Measured result |
|---|---|---|
| 1. Data in | Fixed-seed generator with every planted problem; landing volume; bronze via Auto Loader; quarantine | 422 files, each loaded once; a re-run ingests 0 rows; the re-sent duplicate loads once; all 8 corrupt rows quarantined with the right reason |
| 2. Silver | Config-driven mapping to one position model; security master resolution; units rule; dedupe | 86,667 bronze rows = 85,995 silver + 193 quarantined + 475 unresolved + 4 deduplicated, file by file against ground truth; identical after re-run and full refresh |
| 3. Gold | Trusted positions in USD, exposure, reconciliation, AI/BI dashboard | All 11 planted breaks found with the right reason, 0 unexplained; the late file shows as pending, then clears; tie-out 97.94 % |
| 4. Onboarding by config | One Lakeflow Job per client on file arrival; a fourth custodian and a second client | Holloway Custody: 34 YAML lines, 2 generic engine changes, 13.9 min delivery to validated gold. Wrexford: 109 YAML lines, no engine change, 12.7 min |
| 5. Scale | 1M, 5M and 15M rows; two optimisations tried | At 15M rows bronze went from 311 s to 147 s with parallel ingest (kept); liquid clustering by date gave no measurable gain (dropped) |
| 6. AWS | S3 landing zone, validating forwarder Lambda, SQS dead-letter queue, CloudFormation | S3 upload to validated gold in 696 s; duplicates skipped, bad files quarantined, a simulated outage dead-lettered; everything torn down |
| 7. AI assistant | Read-only agent drafts a source's YAML; dry run; human approval | Leave-one-out on 8 sources: 8/8 valid, 8/8 pass the dry run, 127/128 fields correct, $0.178 |

## Design decisions

- **Medallion layers.** Bronze keeps every file as received (plus source file, load time and content hash), silver standardises, gold holds the business tables. Nothing is ever lost, and each step can be tested on its own.
- **Exactly once.** Auto Loader checkpoints load each file path once; a SHA-256 of each file's bytes catches the same file re-sent under a new name; every Delta write is idempotent per micro-batch.
- **Onboarding is configuration.** Each source is a YAML file (format, separators, date format, identifier, units, column mapping); each client is a YAML file listing its sources. A script renders each client's pipeline, job and dashboard from its YAML.
- **One schema per client.** Each client has its own Unity Catalog schema, volumes, pipeline and job; a test proves one client's pipeline reads nothing from another's schema, and Unity Catalog lineage confirms it on Databricks.
- **Never drop a row.** Rows that fail a rule go to quarantine with the reason; identifiers that do not resolve go to an unresolved queue; reconciliation separates real breaks from rows already held upstream (`explained_upstream`).
- **Proof built in.** The generator plants every problem with a known answer and writes ground truth to a separate `qa` schema that pipeline code may not read. Every job ends by validating against it.
- **Declarative pipeline for silver and gold.** AUTO CDC (the pipeline form of MERGE) on natural keys for silver, materialized views for gold, so re-runs and full refreshes give identical tables.
- **The AI assistant only drafts.** Read-only tools, a validated draft, a dry run, and a person approves before any config is written.

## What onboarding taught the engine

Adding a fourth custodian and a second client changed only YAML, except for two generic engine changes Holloway Custody's format needed: **any day, month and year order for dates** (it sends `dd.MM.yyyy`), and **decimal and thousands separators** (it writes `3.802.151,16`; without this a quantity like `38.700` would have been read as 38.7). The second client needed none. Details, plus the platform tooling added for many clients (the render script, the pipeline-slot wait, delivery-aware validation), are in [Slice 4](#slice-4-orchestration-and-onboarding-by-config).

## Findings: Databricks Free Edition and AWS

What did not work as expected, and what was done about it:

| Where | Finding | What we did |
|---|---|---|
| Free Edition | Serverless compute could not reliably read bundle files under `/Workspace` (403 from workspace storage) | Jobs are notebook tasks that install the project wheel from a Unity Catalog volume and read YAML from it ([details](#workspace-file-limitation-and-the-wheel-in-a-volume-workaround)) |
| Free Edition | One active pipeline per type | One pipeline per client; each client job waits for a free slot before starting its pipeline |
| Free Edition | Deleting a pipeline leaves its tables owned by it during a restoration period, so a replacement pipeline is refused | Moved the tables with `ALTER ... SET TBLPROPERTIES ('pipelines.pipelineId' = ...)`, then a full refresh |
| Free Edition | File-arrival triggers on volumes fire only for new files: not for overwrites, and not for files already there at the trigger's first check | Deliveries start a few minutes after a deploy; a re-sent file under the same name is a no-op by design |
| Free Edition | No Spark UI on serverless | Measured from job timings, the pipeline event log, Delta details and query history instead |
| Free Edition | Databricks cannot read an S3 bucket in your own account (no custom storage locations) | A forwarding Lambda puts files into the volume; production would use a Unity Catalog external location |
| SQL warehouse | The result cache ignores comments and constant-folded filters, so repeated probe queries returned cached results | Each probe adds a different constant output column |
| Databricks | A service principal needs the `workspace-access` entitlement before any API works | Granted by the identity script |
| AWS | New-experience accounts are pinned to their home Region by AWS-managed service control policies (us-east-1 denied, home us-east-2 allowed) | Everything runs in us-east-2; found the hard way |
| AWS | S3 tag values reject quotes and `*` | Tag values are reduced to allowed characters; the full reason stays in object metadata |

## Documents

- [docs/DISCOVERY.md](docs/DISCOVERY.md): the client's problem, users and success metrics, and how the project measured up
- [docs/RUNBOOK.md](docs/RUNBOOK.md): what to do when a custodian file is late, a header changes, unmapped securities spike, the Free Edition quota is hit, or the AWS forwarder dead-letters
- [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md): the original spec

## Repository layout

```
config/clients/<client>.yml     which sources a client has, and its Unity Catalog schema
config/sources/<source>.yml     format, delimiter, columns, quarantine rules, header aliases
src/keelstone/generator/        fixed-seed generator that plants every problem in spec section 3
src/keelstone/ingest/           generic bronze ingest (one Auto Loader stream per configured source)
src/keelstone/silver/           silver mapping (config to SQL), pipeline definitions, local mirror
src/keelstone/gold/             reconciliation rules and gold pipeline definitions
src/keelstone/assistant/        AI onboarding assistant: read-only tools, agent loop, draft + dry run
onboarding/audit.jsonl          audit log of every draft, approval and rejection
dashboards/                     AI/BI dashboard template and per-client dashboards (generated)
pipelines/                      Lakeflow declarative pipeline source (thin; logic is in the wheel)
src/keelstone/qa/               where ground truth lives; pipeline code may not import it
jobs/                           Databricks notebooks: setup, ingest, validation
resources/clients/              per-client pipeline, job and dashboard (generated from the client YAML)
scripts/                        generate, deliver (upload by delivery date), build wheel, deploy
aws/                            S3 landing zone: CloudFormation template, forwarder Lambda, deploy/teardown, e2e checks
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

## Slice details

### Slice 1: data in

- **Bronze is config-driven.** `ingest_bronze` reads the client YAML and starts one Auto Loader stream per listed source from that source's YAML. No source-specific code.
- **Exactly once.** Auto Loader checkpoints load each file path once. A file re-sent under a new name is caught by a SHA-256 of its bytes and logged as `duplicate_content`. Every Delta write uses `txnAppId`/`txnVersion`, so a retried micro-batch is not written twice.
- **Nothing silently dropped.** Every row lands in the source's bronze table or in `bronze_quarantine` with a reason (`malformed_record`, `missing_required:<col>`, `not_numeric:<col>`, `bad_date:<col>`). Every file gets one row in `bronze_file_log`.
- **Schema drift keeps loading.** Columns are matched by header name. Unknown columns go to `_rescued_data` and are logged per file in `bronze_schema_drift_log`. A known rename is mapped by a `column_aliases` entry in the source YAML.
- **Ground truth stays out of the pipeline.** It lives locally in `data/ground_truth/` and in the `keelstone.qa` volume. A test fails if ingest (or later silver and gold) code references it. Only validation jobs read it.

### Slice 2: silver

- **Silver is a Lakeflow declarative pipeline.** A check on Free Edition showed a serverless pipeline installs the project wheel from the volume (`environment.dependencies`), loads its source file, records expectation counts in the event log, and AUTO CDC (the pipeline form of MERGE) keeps re-runs unchanged. So silver uses one pipeline rather than plain PySpark jobs.
- **Mapping is config.** Each source YAML has a `silver:` section: which bronze column feeds each canonical field, the identifier type and columns, and the units multiplier. Dates use the format already in `rules.dates`. Pemberton's quantity is `lots[].quantity`, summed per position. Linden's renamed headers are `header_versions` in its YAML; bronze aliases are derived from them and every silver row records its `header_version`.
- **One position model.** `silver_positions`: client_id, source_id, position_side, account_id, security_id, business_date, quantity, price, market_value (local currency, after the multiplier), currency, identifier_type, identifier, header_version, source_file. Natural key: client, source, account, security, business date. The book of record maps into the same model (no price or value; gold values it).
- **Security master resolution.** `silver_identifier_map` maps ISIN, CUSIP, ticker:exchange and security_id to one security_id. An identifier with no match, or more than one, goes to `unresolved_securities` with its source and identifier, never dropped.
- **Units rule.** A row whose market value / (quantity x price) is between 500 and 2000, or between 0.0005 and 0.002, is quarantined in `silver_quarantine` with reason `units_scale_suspect`.
- **Dedupe.** Identical rows within a file are collapsed by the natural-key MERGE; `silver_dedupe_log` records how many per file. `silver_key_conflicts` lists any natural key seen with different content, and must stay empty.
- **Accounting.** For every file: bronze rows = silver + silver quarantine + unresolved + deduped. The Slice 2 job checks this, compares each part with the ground truth, and proves idempotency by snapshotting every silver table after an update, a re-run and a full refresh.

### Slice 3: gold

- **Gold is in the same pipeline as silver**, as materialized views over the silver tables. Free Edition allows one active pipeline per type; adding datasets to the existing pipeline needs no second one. Materialized views are recomputed from silver, so re-runs leave them unchanged.
- **Trusted positions.** `gold_positions` is the client's book of record per business date, valued at closing prices, converted to USD with the FX table, with each position's reconciliation status attached.
- **Exposure.** `gold_exposure_asset_class` and `gold_exposure_currency`: market value in USD (and local currency for currencies) with weights, per client and date.
- **Reconciliation.** `gold_reconciliation` compares the book with the custodians per account, security and date. Breaks: `quantity_break`, `value_break` (beyond the larger of an absolute and a relative tolerance set in the client YAML), `missing_at_custodian`, `missing_in_book`. If a custodian's file for that date has not arrived, the book position is `pending_late_file` (status pending, not a break) until it does.
- **Nothing unexplained.** A book position whose custodian row was held back upstream is not reported as missing. It gets status `explained_upstream` with the queue that already holds it: `custodian_row_rejected` (bronze quarantine), `custodian_row_quarantined` (silver quarantine) or `custodian_identifier_unresolved` (unresolved securities). A rejected row whose key cannot be read is attributed by count within the same custodian, date and account (`explanation_basis = count`).
- **Tie-out rate** = matched / (compared minus pending), per date in `gold_tieout_daily`.
- **Measured on the slice 3 dataset (three custodians, 60 business days, seed 42; slice 4 results with Holloway Custody are below):** 33,374 positions compared; 32,687 matched; 11 breaks (4 quantity, 3 value, 2 missing at custodian, 2 missing in book, all planted); 676 explained upstream; 0 pending once every file arrived. Tie-out rate 97.94% overall, 98.78% on the last date, lowest 63.93% on 2026-08-25 (the day Linden sent values in units, quarantined as `units_scale_suspect`). Before the late Pemberton file arrived, its 187 book positions showed as pending and the tie-out rate was 98.50%.
- **Dashboard.** `dashboards/gold_overview.lvdash.json` (AI/BI) is deployed by the bundle: tie-out rate over time, exposure by asset class and currency, non-matched positions by reason, and the break queue.

### Slice 4: orchestration and onboarding by config

#### Onboarding by config: the numbers

| | Holloway Custody (4th custodian for Calder Valley) | Wrexford County Firefighters' Pension Fund (2nd client) |
|---|---|---|
| **Lines of YAML** | **34**: `config/sources/holloway.yml` (new, 33) and one line in `config/clients/calder_valley.yml` | **109**: `config/clients/wrexford.yml` (18), `config/sources/linden_wrexford.yml` (31), `quayside.yml` (31), `wrexford_book.yml` (29) |
| **Engine changes needed** | **2, both generic** (see below) | **none** |
| **Delivery to validated gold** | **13.9 min** (full refresh plus the update, re-run and full-refresh idempotency check) | **12.7 min** from first file delivered to validation passed; the file-arrival run itself took 9.2 min |
| Files changed | 2 config files | 4 config files, plus 2 files generated from its YAML by `scripts/render_client_resources.py` (`resources/clients/wrexford.yml`, `dashboards/wrexford.lvdash.json`) |
| Writing the config to a clean local dry run | 3 min 14 s, including finding and fixing the 2 engine gaps | 8 s (written by Claude Code, so not representative of a person) |
| Config ready to validated gold, wall clock | 1 h 31 min. This includes about 18 min of slice 4 platform work before the first deploy and about 45 min lost to the one-time table-ownership problem below, neither caused by the new custodian | 39 min, including the same 18 min of platform work before the first deploy |

Generator and ground-truth additions for the new synthetic data (`src/keelstone/generator/clients.py`, `extra.py`) are not counted as onboarding: they create the test data, not the pipeline. Adding Holloway left every Harrow, Linden and Pemberton file byte-identical (a test checks this against the slices 1 to 3 dataset).

#### What onboarding taught the engine

Two generic engine changes, both needed by Holloway Custody's file format and neither specific to any source:

1. **Any day, month and year order for dates.** The engine accepted only four date formats, and Holloway sends `dd.MM.yyyy`. Config validation now accepts any order of `yyyy`, `MM` and `dd` with one consistent separator (or none); Spark and the local mirror already parsed date patterns generically.
2. **Decimal and thousands separators.** Holloway writes `3.802.151,16`. Without this, every price failed the numeric rule, and worse, a quantity like `38.700` would have passed silently as 38.7. Any source can now set `decimal_separator` and `thousands_separator`. Numbers are normalised before the bronze numeric rule, in every silver cast (including nested tax lots) and in the local mirror; config validation rejects a separator that is also the delimiter.

Platform tooling added in this slice (needed to run many clients, not by any one format):

- **`scripts/render_client_resources.py`** builds each client's pipeline, orchestration job and AI/BI dashboard from its client YAML and one dashboard template. It is generic: it contains no client or source names and reads only the client id, catalog, schema, landing volume and display name. Onboarding a client is its YAML plus this script.
- **`jobs/wait_for_pipeline_slot.py`** runs clients' pipelines one at a time (below).
- **Delivery-aware validation.** The silver and gold validations check only files that have landed, read unresolved identifiers from a generic ground-truth list, and work out pending late files from the landing volume, so they hold for any client on any day.

#### The one-time table-ownership problem, and the fix

Moving from slice 3's single `silver` pipeline to one pipeline per client meant deleting the old pipeline. Databricks keeps a deleted pipeline's tables in a restoration period and still treats them as owned by it, so the new Calder Valley pipeline was refused ("Table ... is already managed by pipeline ..."). Its pipeline task failed twice, about 15 minutes each, before the run was cancelled. Wrexford, a new schema, was not affected. The fix was the one Databricks documents: move each of the 17 silver and gold tables to the new pipeline with `ALTER STREAMING TABLE | MATERIALIZED VIEW ... SET TBLPROPERTIES ('pipelines.pipelineId' = '<new pipeline id>')`, then run the client job with `first_update=full_refresh`. It passed every check. This only happens when a pipeline is replaced, not when a client or source is onboarded; the same move is needed if the `demo` target replaces `dev`.

#### Orchestration

- **One Lakeflow Job per client**, generated from its YAML: file arrival on the client's landing volume, then setup, bronze ingest, wait for a free pipeline slot, the client's own silver and gold pipeline, then bronze, silver and gold validation against the ground truth.
- **File-arrival triggers on Unity Catalog volumes work on Free Edition.** A test file landed at 21:17:23 and the triggered run started at 21:18:38; in the backfill, Wrexford's files triggered its run at 21:47:06. Two limits found: a file that lands before the trigger's first check after a deploy does not trigger a run, and overwriting an existing file with the same name does not trigger one either (only new files do).
- **Client pipelines run in sequence.** Free Edition allows one active pipeline per type (it does allow several to exist). Each client job runs `wait_for_pipeline_slot` before its pipeline task: it lists the other clients' pipelines and waits while any update is active. In the backfill, both jobs triggered within 90 seconds of each other; Calder Valley's job waited 30 seconds for Wrexford's pipeline to finish before starting its own. Pipeline tasks also retry twice, five minutes apart, in case two jobs pass the check at the same moment.
- **Backfill.** All 60 days of both clients were delivered at once (482 and 301 files); each client's trigger ran one job that ingested everything.
- **Re-running a date is idempotent.** All of Wrexford's 2026-08-20 files were delivered again under the same names (overwriting them). The next run ingested 0 files and 0 rows, and its update, re-run and full refresh left all 16 silver and gold tables identical, with every count still matching the ground truth. Calder Valley's run (full refresh, re-run, full refresh) did the same for its tables.
- **Deploy targets.** `dev` (default) prefixes every name with `[dev <user>]`. `demo` has the same resources without the prefix, for screenshots: `KEELSTONE_TARGET=demo python scripts/deploy.py`. Deploy one or the other, not both: two pipelines cannot manage the same tables.

#### Results

| | Calder Valley (4 custodians) | Wrexford (2 custodians) |
|---|---|---|
| Files | 482 (481 loaded, the re-sent duplicate loaded once) | 301 |
| Bronze rows = silver + quarantine + unresolved + deduped | 97,467 = 96,744 + 193 + 524 + 6 | 39,756 = 39,649 + 0 + 103 + 4 |
| Planted breaks found with the right reason | 15 of 15 (5 quantity, 4 value, 3 missing at custodian, 3 missing in book) | 8 of 8 (2 of each kind) |
| Unexplained rows | 0 | 0 |
| Tie-out rate | 98.07% (lowest 68.08% on the Linden units-error day) | 98.88% |
| Idempotency (update, re-run, full refresh) | every silver and gold table identical | every silver and gold table identical (after re-delivering a date) |
| Lineage reads from another client's schema | none (1,003 lineage records checked) | none (115 lineage records checked; the first run found none yet, as lineage arrives with a delay) |

#### Client isolation

Each client has its own schema, landing volume, checkpoint volume, pipeline and job. `tests/test_client_isolation.py` builds each client's pipeline with recording stand-ins for Spark (no Spark needed), runs every dataset definition, and asserts that every table it reads is its own pipeline dataset or a table in its own schema, and never the other client's. On Databricks, the gold validation also checks Unity Catalog lineage for reads from other schemas.

### Slice 5: scale and performance

**Setup.** A separate `scale_test` client (own schema, landing volume, pipeline, job and dashboard), so the demo clients' tables and dashboards are untouched. The same generator in scale mode (`python scripts/generate.py --client scale_test --rows N`) streams 60 business days of clean data: a Harrow-layout CSV custodian, a Pemberton-layout JSON custodian with nested tax lots, and a matching book of record, all gzipped, plus the reference files. Its sources are new YAML files only. Each size was run through the normal client job (bronze, the silver and gold pipeline, every validation), then a `measure_performance` task recorded the numbers, and `scripts/probe_queries.py` timed four read queries on the SQL warehouse (2X-Small) with the result cache defeated. Every run passed every validation: all rows accounted for, every position tied out.

| Size | Custodian + book rows | Position lots | Accounts | Gzipped files |
|---|---|---|---|---|
| 1M | 1,003,896 | 513,444 | 84 | 25 MB |
| 5M | 4,976,912 | 2,530,535 | 417 | 107 MB |
| 15M | 14,933,118 | 7,610,960 | 1,250 | 311 MB |

#### Runtimes (all measured; numbers from `docs/perf/*_run.json`, table built by `scripts/perf_summary.py`)

| Size | Run | Rows ingested | Bronze s | Bronze rows/s | Pipeline s (wall) | Silver executor s | Gold executor s | Slowest flow (executor s) | Bronze files per table |
|---|---|---|---|---|---|---|---|---|---|
| 1M | baseline | 1,126,076 | 225.5 | 4994 | 149.7 | 70.0 | 10.4 | silver_positions (30) | 2 |
| 5M | baseline | 5,099,092 | 212.6 | 23984 | 173.2 | 190.6 | 30.3 | silver_positions (110) | 2 |
| 15M | baseline | 15,055,298 | 311.2 | 48378 | 263.0 | 619.0 | 86.4 | silver_positions (403) | 2-3-4 |
| 15M | + parallel bronze ingest | 15,055,298 | 146.9 | 102487 | 251.5 | 605.7 | 98.9 | silver_positions (397) | 2-3-4 |
| 15M | + liquid clustering by business_date | 0 (bronze already loaded) | 60.6 | n/a | 266.2 | 608.3 | 122.8 | silver_positions (396) | 2-3-4 |
| 15M | kept: parallel ingest, no clustering | 0 (bronze already loaded) | 55.6 | n/a | 270.9 | 625.6 | 89.2 | silver_positions (411) | 2-3-4 |

"Rows ingested" includes the reference files. Silver and gold executor seconds are the pipeline event log's `executor_time_ms` summed per layer (work across all executors, so it can exceed wall-clock time).

#### Read queries (median of 3 runs, last run's bytes and files; `docs/perf/*_probes.json`)

| Size | Run | latest_day_exposure | one_week_recon_status | one_day_silver | one_account_history |
|---|---|---|---|---|---|
| 1M | baseline | 428 ms, 2.7 MB, 1 files (0 pruned) | 438 ms, 0.3 MB, 1 files (0 pruned) | 482 ms, 1.5 MB, 1 files (0 pruned) | 455 ms, 1.5 MB, 1 files (0 pruned) |
| 5M | baseline | 681 ms, 12.4 MB, 1 files (0 pruned) | 492 ms, 0.3 MB, 1 files (0 pruned) | 611 ms, 12.1 MB, 2 files (0 pruned) | 573 ms, 12.0 MB, 2 files (0 pruned) |
| 15M | baseline | 750 ms, 37.9 MB, 1 files (0 pruned) | 565 ms, 0.3 MB, 1 files (0 pruned) | 673 ms, 36.7 MB, 5 files (0 pruned) | 603 ms, 37.3 MB, 5 files (0 pruned) |
| 15M | + liquid clustering by business_date | 708 ms, 15.1 MB, 1 files (1 pruned) | 681 ms, 0.3 MB, 1 files (0 pruned) | 785 ms, 36.7 MB, 5 files (0 pruned) | 673 ms, 37.3 MB, 5 files (0 pruned) |

#### What the measurements show

- **Bronze was the slowest stage at every size, and mostly fixed cost.** The 1M run took longer than the 5M run (226 s against 213 s): the eight source streams ran one after another, each paying for stream start-up, micro-batch planning and hashing every file's bytes, so throughput rose from 5,000 to 48,000 rows per second as the data grew.
- **In the pipeline, the slowest flow is the AUTO CDC MERGE into `silver_positions`**: about 55 % of the pipeline's executor time at 15M (403 of 737 executor seconds). Gold is cheap by comparison (86 executor seconds at 15M). Every gold materialized view was refreshed by complete recompute (the event log's `planning_information`).
- **At small sizes the pipeline is mostly fixed overhead.** At 1M the update took 150 s of wall-clock time while its longest flow took 19 s and all flows together used 91 executor seconds; the rest is start-up and planning (not broken down further here).
- **Small files were not a problem**: bronze tables have 2 to 4 files each even at 15M, so there was nothing to fix there and no change was made.
- **No skew to fix.** In `silver_positions` at 15M the largest business date has 1.01 times the median row count, the largest account 1.1 times, the largest security 1.43 times.
- **Run-to-run noise.** The pipeline ran four times on the identical 15M data: 251.5 s to 270.9 s (about 7.5 % apart). Differences smaller than that are not real.
- **What is not available.** Serverless compute has no Spark UI, so there are no stage, task or shuffle-size numbers here. What is available and used: job task timings, the pipeline event log (per-flow rows, wall time and executor time, refresh technique), Delta table details (files, bytes, clustering), and query history metrics for the read queries (time, bytes and files read and pruned).

#### Optimisations, before and after on the same 15M data

| Change | Before | After | Kept? |
|---|---|---|---|
| **Parallel bronze ingest**: start every source's Auto Loader stream at once instead of one after another (`performance.parallel_ingest: true` in the client YAML) | bronze 311.2 s (48,378 rows/s) | bronze 146.9 s (102,487 rows/s), **2.1 times faster**; all validations pass | **yes** |
| **Liquid clustering by `business_date`** on the date-keyed silver and gold tables (`performance.cluster_by: [business_date]`) | pipeline 251.5 s; latest-day exposure 750 ms reading 37.9 MB | pipeline 266.2 s (within noise); latest-day exposure 708 ms reading 15.1 MB (1 file pruned); one-week recon 565 to 681 ms and one-day silver 673 to 785 ms (slower, no files pruned: the MERGE-written table was not laid out by date) | **no**: fewer bytes for one query but no measurable time gain at this size on a 2X-Small warehouse. The setting stays available in YAML; it should be re-tested at larger sizes |

Both are generic, config-driven engine features (any client can switch them in its YAML). Re-running Calder Valley's and Wrexford's jobs after this slice found one validation gap (not caused by the slice 5 changes): the silver check compared expectation counts from the latest pipeline update only, and an incremental update with no new data records none. It now sums them over every update since the last full refresh; both clients then passed bronze, silver and gold validation. The final `scale_test` run with the kept configuration (parallel ingest on, clustering off) passed every validation; its pipeline took 270.9 s.

### Slice 6: AWS landing zone

**Why a forwarding Lambda.** In production, Databricks would read the S3 bucket directly: a Unity Catalog storage credential (an IAM role) and an external location on the bucket, with Auto Loader and the file-arrival trigger pointed at the S3 path. Databricks Free Edition has no account console and does not allow custom storage locations, so it cannot read a bucket in your own AWS account. This project therefore lands files in S3 and forwards them into the client's Unity Catalog volume through the Databricks Files API; from there the existing file-arrival trigger, pipeline and validations run unchanged. The forwarder also does the checks a landing zone should do before anything reaches the lakehouse.

```
custodian file -> s3://keelstone-landing-<account>-us-east-2/incoming/<client>/<source>/<date>/<file>
  -> S3 event -> Lambda (Python 3.13, standard library + boto3)
       check against the source YAML: client and source known, file name matches file_glob,
       non-empty, readable (gzip opens, first line is UTF-8); SHA-256
       bad  -> quarantine/<same key>, tagged and annotated with the reason; original removed
       good -> Files API PUT into /Volumes/keelstone/<client>/landing/<same path>, overwrite=false
               (409 = already there: a duplicate, skipped)
       Databricks unreachable or refusing -> exception -> 2 retries -> SQS dead-letter queue
  -> the client's file-arrival trigger -> bronze -> silver and gold pipeline -> validation
```

**Infrastructure as code.** `aws/template.yaml` (one CloudFormation stack, deployed by `aws/deploy.sh` with the AWS CLI): the landing bucket (public access blocked, SSE-S3 encryption, bucket owner enforced, TLS-only policy, lifecycle deleting every object after 7 days), the Lambda with an async retry limit of 2 and the SQS dead-letter queue, a log group kept 7 days, and an IAM role that may only read and delete under `incoming/`, write under `quarantine/`, read the one SSM parameter, send to the one queue and write its own logs. The workspace URL is a stack parameter given at deploy time, never committed. `aws/teardown.sh` deletes the stack, both buckets, the SSM parameter and the Databricks identity.

**Secrets and identity.** The Databricks token lives only in SSM Parameter Store as a SecureString (`/keelstone/databricks/token`), written by `aws/databricks_identity.py` straight from the Databricks API (never printed or stored in the repo). The token belongs to a dedicated Databricks service principal, `keelstone-landing-forwarder`, with only the `workspace-access` entitlement (the Files API refuses identities without one) and Unity Catalog grants of `USE CATALOG`, `USE SCHEMA` and read/write on one client's landing volume; it expires after 7 days. The Lambda reads it from SSM at run time and caches it per container.

**Tests.** `tests/test_aws_forwarder.py`: 17 local unit tests with S3, SSM and HTTP stubbed (no AWS calls): routing from the YAML, every quarantine reason, gzip, URL-encoded keys, the exact Files API request, the 409 duplicate path, and failures that must raise so Lambda retries and dead-letters.

**End-to-end run on AWS (us-east-2), measured.** A separate `wrexford_e2e` client (Wrexford's source YAMLs in its own schema) received days 1 to 59 the usual way and was validated, then day 60 was uploaded to S3:

| Check | Result |
|---|---|
| Day 60 (5 files) uploaded to S3 | all 5 files in the Databricks landing volume 3.5 to 5.0 s after the upload started |
| File-arrival trigger | job started 161 s after the upload; bronze loaded exactly those 5 files, once |
| Validation | bronze, silver and gold passed through 2026-09-22 (all 60 days), tie-out 99.05 % |
| **S3 upload to validated gold** | **696 s (11.6 min)** |
| Duplicate upload (same object again) | forwarder logged `duplicate` (Databricks returned 409, same SHA-256); nothing loaded twice |
| Bad files | an empty file and a misnamed file moved to `quarantine/` with their reasons; originals removed |
| Databricks outage (stack parameter `SimulateOutage=true`) | the event went to the SQS dead-letter queue after 2 retries, 196.8 s after upload, with "cannot reach Databricks" |
| Teardown (`aws/teardown.sh`) | stack, Lambda, queue, log group, role, both buckets and the SSM parameter deleted; Databricks token revoked and service principal deleted; checked afterwards. The `wrexford_e2e` client was then removed from Databricks (`deploy: false` in its YAML) |

AWS cost: estimated under $0.05 for the whole slice, all inside free-tier allowances (estimate from list prices and the usage above; not checked against the billing console).

**Findings (the hard way).**

1. **New-experience AWS accounts are tied to a home Region by AWS-managed service control policies.** This account's home Region is us-east-2 (Ohio). In us-east-1 every regional service this slice needs (S3, Lambda, SQS, SSM, Secrets Manager, CloudFormation) was refused with "explicit deny in a service control policy", even though the role has AdministratorAccess; the managed SCPs only allow global and control-plane services in us-east-1 and us-west-2. We first read it as a custom restriction; the IAM policy simulator with `aws:RequestedRegion=us-east-2` then showed every needed action allowed, and everything was moved to us-east-2.
2. **A service principal needs an access entitlement before any API works.** The first forward of day 60 got HTTP 403 ("API is disabled for users without the ... workspace-access ... entitlements"). The Unity Catalog grants alone were not enough. The failure path worked as designed (2 retries, then the dead-letter queue, files kept in S3); after adding `workspace-access` the same files were uploaded again and went through. The identity script now grants it.
3. **S3 tag values reject quotes and `*`.** The misnamed file's reason ("does not match quayside pattern '*.tsv'") made `CopyObject` fail with InvalidTag, so it was retried and dead-lettered instead of quarantined. The unit tests' fake S3 did not enforce tag rules; it does now, and the tag value is reduced to allowed characters (the full reason stays in the object metadata).
4. Outside us-east-1, `s3api create-bucket` needs an explicit `LocationConstraint`; the deploy script now sets it.

**Production notes.** A production setup would (a) let Databricks read the bucket directly through a Unity Catalog storage credential and external location instead of forwarding files, and (b) deploy with a narrowly scoped CI/CD role limited to this stack's resources, not the account-wide `AccountFullAccessRole` (AdministratorAccess) used here from the CLI.

### Slice 7: AI onboarding assistant

**What it does.** Given one sample file from a new source, an agent (Claude Sonnet 5.5 through the Anthropic SDK) drafts that source's YAML config: format, delimiter, decimal and thousands separators, date format, identifier type and columns, units multiplier, and the mapping into the canonical position model.

**How it is kept safe.**
- **Read-only, typed tools only** (`src/keelstone/assistant/tools.py`, all `strict` schemas): describe the sample, read lines, sniff the delimiter, list columns (including nested lots), profile a column (sample values, number styles, date patterns, identifier shapes), check what share of identifiers match the security master as ISIN, CUSIP, security_id or ticker:exchange, and estimate the units multiplier from value / (quantity x price). No file writes, no shell, no access to any existing config; a test checks the assistant's code has no write or subprocess calls.
- **The output must validate.** The agent ends by calling `submit_draft`; code (not the model) turns it into a config dict and validates it with `config.py`. An invalid draft gets the error back and one retry, then the result is "needs a human, here is what I found" with every tool call as evidence. A refusal, a stop without a draft, or the turn limit end the same way.
- **Dry run before a person sees it.** The draft runs through the plain-Python rules engine on the sample: parse rate at least 98 %, quarantine rate at most 5 %, no mapping errors, unresolved-identifier rate at most 5 %, and for files with price and value a median value / (quantity x price) between 0.98 and 1.02 with at most 1 % of rows units-suspect.
- **A human approves.** `python scripts/onboard_source.py draft | show | approve | reject`: `show` prints the draft YAML, the evidence and the dry run; only `approve` writes `config/sources/<id>.yml`, and it refuses a draft whose dry run failed unless the approver gives a reason. Every draft, approval, rejection and eval run is appended to `onboarding/audit.jsonl`.
- **Budget guard.** Every model call is costed (Sonnet 5.5: $2 input, $10 output, $2.50 cache write, $0.10 cache read per million tokens) and the loop stops before a call that could pass the budget. Prompt caching covers the growing prefix; server-side refusal fallback is enabled.
- **Unit tests** (`tests/test_assistant.py`, 16 tests) use a scripted fake model, no API calls: tools on real generated samples, validation, the dry run catching a wrong multiplier and a wrong date format, the retry, the fallback, refusal, the budget stop, and approval as the only path that writes config.

**Leave-one-out evaluation (one run, Claude Sonnet 5.5).** For each existing position source, the agent drafted a config from one ordinary day's file (2026-07-08) without seeing any config, and each field was scored against the real one (`scripts/eval_assistant.py`, results in `docs/eval/assistant_eval.json`):

| Source (layout) | format | delim | dec sep | thou sep | date fmt | id type | id cols | units x | account | date col | qty | price | value | ccy | columns | numeric | Dry run | Score | Cost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| harrow | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | pass | 16/16 | $0.022 |
| linden | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | pass | 16/16 | $0.024 |
| pemberton | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✗ | pass | 15/16 | $0.023 |
| holloway | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | pass | 16/16 | $0.023 |
| linden_wrexford | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | pass | 16/16 | $0.022 |
| quayside | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | pass | 16/16 | $0.024 |
| book_positions | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | pass | 16/16 | $0.020 |
| wrexford_book | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | pass | 16/16 | $0.021 |

Per field: format 8/8 · delim 8/8 · dec sep 8/8 · thou sep 8/8 · date fmt 8/8 · id type 8/8 · id cols 8/8 · units x 8/8 · account 8/8 · date col 8/8 · qty 8/8 · price 8/8 · value 8/8 · ccy 8/8 · columns 8/8 · numeric 7/8

**Result: 8 of 8 drafts valid on the first submission, 8 of 8 passed the dry run, 127 of 128 fields correct.** Total cost **$0.178** (24 model calls), against an estimate of $1.58: the agent made 6 or 7 tool calls per source but in parallel, so it needed 3 model calls per source instead of the 10 assumed, and most of the prompt was read from the cache.

**The one miss.** Pemberton, `numeric_columns`: the draft also marked `lots[].cost_basis` as numeric. The values are numeric, so it is harmless (one more quarantine rule), but it is not what the real config says.

**What this does not show (honest limits).**
- The samples come from this project's own generator, whose formats the engine was built around; real custodian files are messier.
- The system prompt's example of comma decimals (`3.802.151,16`) happens to be in Holloway's exact style, which may have helped on Holloway's separators.
- One sample cannot reveal things only history shows: Linden's real config also carries versioned header aliases for a header change that happens later in the period; the draft (from a day before the change) does not, and the eval does not score it. Likewise required-field lists and display names are not scored.
- One run, one model, eight sources: a small sample, not a benchmark.

## Databricks Free Edition notes

- Serverless only, no retries in dev, small data steps, so a quota error stops the run rather than looping.

### Workspace-file limitation and the wheel-in-a-volume workaround

**Limitation.** On this Free Edition workspace, serverless compute could not reliably read bundle files under `/Workspace`. A `spark_python_task` script, and in one run an `import` of a module from the bundle's `src/`, failed with `OSError: [Errno 5]` (a 403 from workspace storage). The same files read fine through the API, and retries failed the same way, so it is a platform issue rather than our code. Notebook tasks load fine, and Unity Catalog volumes read and write reliably.

**Workaround.** `scripts/deploy.py` builds a wheel of `src/keelstone` (standard library only) and uploads it, with `config/`, to the volume `keelstone.platform.artifacts`, then runs `databricks bundle deploy`. Every job task is a notebook whose first cell is `%pip install` of that wheel, and it reads YAML from the volume. Onboarding a new source is still config only: edit YAML, run `deploy.py`.
