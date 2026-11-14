# Discovery: Calder Valley Teachers' Retirement System

> Everything here is fictional: the client, its team, the custodians and the figures are invented for this project, and the data is synthetic.

## Who

The client's **investment operations team: 6 people (fictional)**.

| Role (fictional) | What they need from the data |
|---|---|
| Head of investment operations | A daily answer to "do we hold what we think we hold?", and the onboarding timeline for new custodians |
| Two reconciliation analysts | A list of what does not tie out, with the reason, early in the day |
| Data analyst | One consistent position model across custodians, and exposure by asset class and currency |
| Custodian relationship manager | Evidence when a custodian's file is late, malformed or wrong |
| Operations engineer (part time) | Something they can run and fix without a vendor |

## Today

- **Three custodian files every morning** (Harrow Trust CSV, Linden Bank pipe-delimited, Pemberton Custody JSON), each with its own identifiers, date format and units, matched against the book of record in **spreadsheets**.
- About **1 % of positions break** on a typical day, and breaks are found late, often the next day.
- **Onboarding a custodian takes 8 to 12 weeks** of custom work; each new format is a new script.
- Known pains: Linden reports values in thousands and occasionally changes its header; Pemberton splits positions into tax lots; some identifiers are missing from the security master; files are sometimes re-sent or late.

## What they asked for

1. Load every custodian's file reliably, whatever its format, so that adding the next custodian or client is a configuration file, not new code.
2. One trusted position book and exposure per day.
3. Reconciliation that separates real breaks from known data problems, with reasons.
4. Nothing silently dropped, ever.
5. A way to onboard a new source quickly and safely.

## Success metrics, in priority order, and what the project measured

| # | Metric | Target | Measured (synthetic data) | Met? |
|---|---|---|---|---|
| 1 | **Time to onboard a new custodian or client** | A config file and one validated run, under a day | Holloway Custody (new custodian, new format): 34 lines of YAML, 13.9 min from delivery to validated gold, plus two one-off generic engine changes. Wrexford (new client): 109 lines of YAML, no engine change, 12.7 min. The AI assistant drafts a source's YAML from one sample: 128 of 128 fields correct across 8 known sources, with a prompt tested to contain no value from their configs | Yes |
| 2 | **Pipeline runtime at scale** | Measured | 15 million rows: bronze 147 s (after parallel ingest; 311 s before), silver and gold pipeline about 250 to 270 s | Measured |
| 3 | **Zero silent drops** | Every input row in silver, quarantine or the unresolved queue | Checked after every run, file by file: bronze rows = silver + quarantine + unresolved + deduplicated, matching ground truth | Yes |
| 4 | **Positions that tie out** | As high as the data allows, every exception explained | Calder Valley 98.07 %, Wrexford 98.88 %; every non-matched position is a planted break or explained upstream; 0 unexplained | Yes |
| 5 | **Breaks found the same day** | Same day | A file's arrival triggers the job; validated gold about 10 to 14 minutes after the files land (11.6 min end to end through AWS) | Yes |

## Out of scope

Real custodian connections (SFTP, SWIFT), trades-to-positions roll-forward, corporate actions, intraday positions, and access control between people inside one client's team.

## Assumptions to confirm with a real client

- One book of record per client, delivered daily, using the platform's internal security ids.
- Custodian files arrive once per business day; a late file arrives within one business day.
- Value tolerances for reconciliation are set per client (in its YAML) by the operations team.
