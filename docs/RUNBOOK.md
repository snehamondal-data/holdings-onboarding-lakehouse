# Runbook

For the operations engineer on call. Replace `<client>` with the client's schema (for example `calder_valley`). SQL runs on the SQL warehouse; commands run from the repository root. All data in this project is synthetic.

Where to look first, for any problem:

```sql
SELECT * FROM keelstone.<client>.gold_tieout_daily ORDER BY business_date DESC LIMIT 5;    -- breaks, pending, upstream
SELECT * FROM keelstone.<client>.bronze_file_log ORDER BY load_ts DESC LIMIT 20;           -- what arrived, how many rows
SELECT check_name, passed, checked_at FROM keelstone.qa.validation_results
 WHERE client_id = '<client>' ORDER BY checked_at DESC LIMIT 5;                            -- did the last run validate
```

---

## 1. A custodian file is late

**Symptoms.** The tie-out table shows `pending` for a date; the break queue lists that custodian's positions as `pending_late_file`. This is expected behaviour, not a break: the book's positions wait for the custodian's file.

**Check.**
```sql
SELECT custodian, business_date, count(*) AS positions
FROM keelstone.<client>.gold_break_queue WHERE status = 'pending' GROUP BY 1, 2;
SELECT file_path, load_ts FROM keelstone.<client>.bronze_file_log
WHERE source_id = '<custodian>' ORDER BY load_ts DESC LIMIT 3;                 -- last file that did arrive
```

**Do.**
1. Ask the custodian when the file will come (the relationship manager owns this).
2. Do nothing in the platform: when the file lands in the client's landing volume, the file-arrival trigger runs the job, and the pending positions either clear or become real breaks with a reason.
3. If the file arrives under a different name than usual, check it matches the source's `file_glob` in its YAML; if not, it will not be picked up.

**Do not** mark the positions as matched by hand or re-run the job repeatedly: it cannot change the result until the file exists.

---

## 2. A custodian changes its header (schema drift)

**Symptoms.** Rows keep loading (unknown columns go to `_rescued_data` instead of failing the load), but the drift log has entries, and if a renamed column was one we map, silver values or quarantine counts change (for example `missing_required:<column>`).

**Check.**
```sql
SELECT source_id, file_path, unexpected_columns, aliased_columns
FROM keelstone.<client>.bronze_schema_drift_log ORDER BY load_ts DESC LIMIT 10;
SELECT reason, count(*) FROM keelstone.<client>.bronze_quarantine
WHERE source_id = '<source>' GROUP BY reason;
```

**Do.**
1. Confirm the new header with the custodian (renamed columns, new columns, and from which date).
2. Add a `header_versions` entry to the source's YAML (see `config/sources/linden.yml`): `renames` from the new header name to the configured column, and `added` for new columns. This is configuration only.
3. `python scripts/deploy.py`, then check the draft with the local tests: `.venv/bin/python -m pytest -q`.
4. Files that already loaded under the new header kept their raw values in `_rescued_data`. To re-map them, rebuild that client's bronze from the landed files and refresh the pipeline: `python scripts/reset_client.py --client <client> --yes --drop-volumes --keep-landing`, then `python scripts/run_job.py client_<client> first_update=full_refresh`.

---

## 3. Unmapped securities spike

**Symptoms.** `unresolved_securities` grows; the tie-out table shows more `explained_upstream` positions with reason `custodian_identifier_unresolved`; the silver validation's `security_resolved` expectation shows more failed rows.

**Check.**
```sql
SELECT source_id, identifier_type, identifier, count(*) AS rows, min(business_date), max(business_date)
FROM keelstone.<client>.unresolved_securities GROUP BY 1, 2, 3 ORDER BY rows DESC LIMIT 20;
SELECT count(*) FROM keelstone.<client>.silver_security_master;            -- did the master load?
```

**Common causes and what to do.**
- **The security master did not arrive or is old.** Check `bronze_file_log` for `security_master`; get the current master delivered.
- **New securities** the master does not have yet: add them to the master (from the reference data provider), not to the custodian config.
- **The custodian changed its identifier format** (for example a ticker without exchange, or CUSIP instead of ISIN): check a few rows in bronze; if it changed, update the source YAML's `identifier` (type and columns), deploy, and rebuild that source as in section 2, step 4.
- After fixing the master, refresh the pipeline so earlier rows are resolved again: `python scripts/run_job.py client_<client> first_update=full_refresh` (resolution happens when a row is processed, so old unresolved rows only change on a full refresh).

---

## 4. The Free Edition quota is hit

**Symptoms.** A job or pipeline task fails with a quota, capacity or usage-limit error rather than a data error.

**Do.**
1. **Stop. Do not retry in a loop.** Job tasks do not retry (`max_retries: 0`), except pipeline tasks, which retry twice five minutes apart to cover two clients starting at once. If the error is a quota error, cancel the run: `databricks jobs cancel-run <run_id> --profile keelstone`.
2. Wait for the daily quota to reset (the next day).
3. Re-run one client at a time with the default parameters: `python scripts/run_job.py client_<client>`. Auto Loader checkpoints and the content hash mean files that already loaded are not loaded again, and the pipeline's MERGE and recomputed views make a re-run safe.
4. Spend less: skip `idempotency_check=true` (it runs the pipeline three times), deliver files in larger batches so one run handles them, and keep the scale-test client's runs for when nothing else needs the quota.

---

## 5. The AWS forwarder dead-letters files

**Symptoms.** Files uploaded to the S3 landing bucket do not appear in the client's landing volume, and the SQS queue `keelstone-landing-forwarder-dlq` has messages. (Bad files are different: they go to the bucket's `quarantine/` prefix with a reason and never reach the dead-letter queue.)

**Check.**
```bash
AWS="$HOME/.local/bin/aws --profile keelstone-aws --region us-east-2"
Q=$($AWS sqs get-queue-url --queue-name keelstone-landing-forwarder-dlq --query QueueUrl --output text)
$AWS sqs receive-message --queue-url $Q --max-number-of-messages 10 --message-attribute-names All
$AWS logs filter-log-events --log-group-name /aws/lambda/keelstone-landing-forwarder --filter-pattern ERROR
```
Each message's `ErrorMessage` attribute and the Lambda log say why.

**Common causes.**
- **`HTTP 401` or `403`**: the Databricks token expired (it lasts 7 days) or lost permissions. Create a new one: `.venv/bin/python aws/databricks_identity.py create --client <client>`. A 403 mentioning entitlements means the service principal lacks `workspace-access`; the script grants it.
- **`cannot reach Databricks`**: the workspace was down or unreachable (or the stack was deployed with `SimulateOutage=true`; redeploy with `aws/deploy.sh`).
- **`HTTP 404`**: the client's landing volume does not exist; create it (`scripts/deliver.py --client <client> --ground-truth` creates the schema and volume).

**Re-drive after fixing the cause.** Good files stay in `incoming/` for 7 days, so upload each failed object again to the same key (that sends a new S3 event); the forwarder skips any file that already reached the volume (HTTP 409), so re-driving is safe. Then purge the dead-letter queue: `$AWS sqs purge-queue --queue-url $Q`.

**Clean up.** When the landing zone is no longer needed, `aws/teardown.sh` deletes the stack, both buckets, the SSM parameter, the Databricks token and the service principal.
