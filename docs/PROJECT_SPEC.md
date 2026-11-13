# Project spec: Holdings Onboarding Lakehouse

**Working repo name:** `holdings-onboarding-lakehouse`
**Written 12 Nov 2026; revised the same day: the hero is ONBOARDING AND SCALE, reconciliation is a supporting check (so this doesn't repeat the payments project).** Personal project for FDE and data engineering roles at fintech and investment-technology companies (the BlackRock/Aladdin client-onboarding problem). Built with Claude Code, one slice at a time; each slice ends with tests passing and a commit.
**Architecture picture:** `architecture.png` (same folder). Put it at the top of the README.

---

## 1. The story

I'm a **forward-deployed engineer at Keelstone, a fictional investment-data platform** (think of the kind of platform BlackRock's Aladdin is). Keelstone's growth depends on how fast it can **onboard new clients**: every client arrives with different custodian banks, formats and identifiers. My first client is the **Calder Valley Teachers' Retirement System** (a fictional pension fund); a **second client** follows in slice 4. Its investments are held at **three custodian banks**, and each bank sends a daily holdings file in its own format. The client's own **book of record** (what they believe they own) comes from their trading system.

Today the client's investment-operations team reconciles these by hand, onboarding a new custodian takes months of custom work, and problems ("breaks") are found late. My job:

1. **onboard** every source reliably, whatever its format, so that onboarding the **next** custodian or client is a **configuration file, not new code** (the hero of this project);
2. **scale:** standardise millions of rows into **one position model**, resolving every security identifier to **one security**, with measured Spark performance;
3. **verify** the result: a supporting reconciliation of the book of record against each custodian, so the client can trust the onboarded data;
4. add an **AI onboarding assistant** that drafts the config for a new source from a sample file, with a human approving it;
5. prove it works (planted problems with known answers), and hand over a runbook.

**Everything is synthetic.** Keelstone, the client and the three banks are fictional. Say so at the top of the README.

## 2. Customer discovery (seed for `docs/DISCOVERY.md`)
- **Who:** the client's 6-person investment-operations team (fictional figures; label them).
- **Today:** three custodian files every morning, matched in spreadsheets; ~1% of positions break; onboarding a custodian takes 8 to 12 weeks.
- **Success metrics (in priority order):** **time to onboard a new custodian or client** (target: a config file and one validated run, under a day); **pipeline runtime at scale** (measured); zero silent drops (every input row is in silver, quarantine or the unresolved queue); % of positions that tie out; breaks found the same day.

## 3. The sources and the mess (planted problems = ground truth)

| Source | Format | Identifiers | Quirks |
|---|---|---|---|
| Harrow Trust | CSV | ISIN | quantities in units; occasional duplicate rows |
| Linden Bank | pipe-delimited text | CUSIP | DD/MM/YYYY dates; market values **in thousands**; a header that changes mid-period (schema drift) |
| Pemberton Custody | JSON | ticker + exchange | positions split into nested **tax lots** that must be summed |
| Book of record | CSV (daily) + trades | internal security ID | the client's view |
| Reference | security master, prices, FX | ISIN, CUSIP, ticker → internal ID | some identifiers unmapped on purpose |

**Planted problems** (each generated with a known cause). Onboarding problems come first, because onboarding is the hero:

*Onboarding and data-quality problems:*
1. **Schema drift:** Linden's header changes mid-period; ingestion must keep working and flag it.
2. **Units error:** Linden's values-in-thousands not applied; a data-quality rule must catch it.
3. **Unmapped security:** an identifier missing from the security master goes to an "unresolved" queue, never silently dropped.
4. **Duplicate file:** the same file delivered twice loads **once**.
5. **Corrupt rows:** malformed rows go to quarantine with the reason.

*Reconciliation checks (supporting, deliberately few):*
6. **Quantity break:** custodian and book disagree on units.
7. **Missing position:** in the book but at no custodian, or the reverse.
8. **Value break:** quantity matches, value off beyond tolerance (stale price).
9. **Late file:** a custodian file arrives a day late; positions show as **pending**, not broken.

## 4. Architecture

```
custodian files + book of record ──► landing zone (S3, or a Unity Catalog volume in phase 1)
        ──► BRONZE (Auto Loader: each file once, raw + source file + load time; bad rows to quarantine)
        ──► SILVER (config-driven mapping per source → one canonical position model;
                    security master resolution; data-quality expectations)
        ──► GOLD (reconciled positions + breaks with reason codes; pending for late files;
                  exposure by asset class and currency)
        ──► per-client positions and exposure · break queue · dashboard
(later) AI onboarding assistant: drafts a new source's YAML config from a sample file; dry run; human approves
Scheduling: Lakeflow Jobs (file-arrival trigger, retries, alerts). Governance: Unity Catalog.
```

### Key design decisions (be ready to defend)
- **Medallion layers** so raw data is never lost (bronze), standardisation is testable (silver), and business tables are stable (gold).
- **Auto Loader** gives exactly-once file ingestion with checkpoints (the same idempotency lesson as the payments project's Kafka replay).
- **Config-driven onboarding (the hero):** each source is a YAML file (format, delimiter, date format, identifier type, units multiplier, column mapping), and each client is a config listing its sources. **A new custodian or a new client = new YAML files.** Slice 4 proves it by onboarding a fourth custodian **and a second client** with no code change, and records how long it took.
- **Multi-client isolation:** each client's data lives in its own schema in Unity Catalog, so one client can never see another's holdings (the same tenant-isolation idea as the six brands at Veiled).
- **Security master resolution** is its own step with an "unresolved" queue; unmapped identifiers are never dropped.
- **Data-quality expectations** at silver (e.g. quantity not null, value scale sane, one row per account-security-date), with failures counted and quarantined.
- **Real Spark scale:** generate enough data (target 10 to 20 million position-lot rows over ~60 days, within Free Edition quotas) that partitioning, shuffles and join strategy matter. Measure before and after one optimisation (broadcast join of the security master, partition pruning by date) using the Spark UI.
- **PySpark + SQL**, written portably; no Scala (not supported on Free Edition).

## 5. Platform decision (AWS)
- **Phase 1 (slices 1 to 5): Databricks Free Edition.** Free, no time limit, serverless only. Limits to respect: serverless compute only, 5 concurrent job tasks, one active pipeline per type, a daily fair-use quota, limited outbound internet, **no custom storage locations** (so the landing zone is a Unity Catalog **volume**, not our own S3, in phase 1).
- **Phase 2 (slice 6): real AWS.** An AWS account with an **S3 landing bucket**, an **IAM role**, and a Databricks workspace that can read it through a Unity Catalog **external location**. Check before starting whether the 14-day express-setup trial supports this, or whether it needs a full AWS-linked workspace, and estimate cost. **Ask Sneha before spending money.**
- **Résumé honesty:** "AWS" only after slice 6 runs on her own S3 bucket. Databricks running on AWS infrastructure behind Free Edition does not count as using AWS.

## 6. Slices
**Slice 1: data in.** Generator (fixed seed) writing daily files for 3 custodians + book of record + reference data, with every planted problem in §3 (9 types) and a ground-truth file kept outside the lakehouse tables. Landing zone volume; bronze via Auto Loader with source metadata; quarantine. **Done when:** every file loads once; re-running ingests 0 new rows; the duplicate file loads once; corrupt rows are quarantined with reasons; local pytest passes.
**Slice 2: silver.** YAML config per source; canonical position model; security master resolution with an unresolved queue; expectations. **Done when:** every input row is accounted for (silver + quarantine + unresolved = bronze); the units-in-thousands error is caught.
**Slice 3: gold.** Reconciliation with reason codes and tolerances; pending for late files; exposure tables; a Databricks dashboard. **Done when:** every planted problem in §3 appears with the right reason, the late file shows as pending, and nothing unexplained appears.
**Slice 4: orchestration + onboarding by config (the headline slice).** A Lakeflow Job with a file-arrival trigger; backfill over the 60 days; onboard a **fourth custodian** and a **second client** (its own custodians and schema) with **only YAML files**. **Done when:** both flow to gold with no code change, client data stays isolated, re-running any date is idempotent, and the README records the onboarding time.
**Slice 5: scale and performance.** Generate the large dataset; record runtimes and the Spark UI before and after one or two optimisations. **Done when:** the README has the measured numbers and what changed.
**Slice 6: AWS.** S3 landing bucket, IAM role, external location; the same pipeline reading from S3. Ask before spending.
**Slice 7: AI onboarding assistant.** Given a sample file from a new source, an agent drafts its YAML config (format, delimiter, date format, identifier type, units, column mapping) using read-only profiling tools on the sample. A **dry run** validates the draft (row counts, data-quality rules, unresolved rate) before a **human approves** it. Evals: hide the configs of the known sources and score how many fields the agent gets right. This is different from the payments copilot: there the AI investigates problems; here it **speeds up onboarding**, the FDE's core job.
**Slice 7b (optional): Reducto-style PDF extraction** of one custodian's monthly statement into bronze.
**Slice 8: docs.** README ("In one minute", the architecture picture, results, design decisions), DISCOVERY, RUNBOOK (late custodian file, schema drift, unmapped spike, quota exceeded).

## 7. Rules
- Synthetic data only, clearly labelled. No real bank, client or fund names.
- **No em dashes** in README or docs. One commit per slice. No secrets committed.
- Résumé honesty: Databricks, Spark and AWS are **project scale**. Never claim a number that wasn't measured.
- Respect Free Edition quotas: generate data in steps, and stop if a quota is hit rather than retrying in a loop.
