# GcpScheduleProcessing

Scheduled GCP batch jobs, deployed as **Cloud Run Jobs** triggered by **Cloud Scheduler**.
ES modules.

This is the GCP-side counterpart to `PmtScheduleProcessing` (an always-on Express +
`node-cron` server on AWS). Here each job is a run-to-completion process: Cloud Scheduler
fires a cron → the Cloud Run Job runs one job → the process exits 0/1.

## First job: `match-library-gcs-export`

A **port of `PmtScheduleProcessing/scripts/match_library_export.js`** — same logic (query
matches → build internal/external CSVs, segment files, and ZIPs → upload), but reading from
**GCP data stores** instead of AWS and writing output to **GCS**:

| Data | AWS original | GCP port |
|---|---|---|
| Company-code library (tenants, banners, flags) | DynamoDB `CompanyCodeLibrary` | **Firestore** `CompanyCodeLibrary` (db `matchlibrary-baas`) |
| Competitor store mapping | DynamoDB `domains` | **Firestore** `competitor_banners` (same doc) |
| Matches | AWS Aurora `matches_{cc}_{base}_{comp}` | **Cloud SQL** (same tables) |
| Tenant mappings / catalog date | AWS Aurora | **Cloud SQL** |
| Output CSV/ZIP | S3 `bungee.productmatching` | **GCS** bucket |

The transform logic (`cleanMatches`, `cleanForExternalData`, segment handling, zips, the
tenant-specific cases for chewy/bjs/ctc/wholefoods) is copied verbatim from the original.
Output key layout is unchanged:
`match_library_export[_test]/company_code=<cc>/year=/month=/day=/<internal|external>/<file>`.

Connection conventions mirror `matchlibrary-baas`: Cloud SQL via a `pg` pool whose creds come
from **GCP Secret Manager** (`secretsManager.pmtAurora`), `search_path = dbENV`; Firestore db
`matchlibrary-baas`.

## Layout

```
index.js                         # dispatcher: runs JOB_NAME, exits 0/1
jobs/
  registry.js                    # every job: schedule, timeout, env, handler
  match_library_gcs_export.js    # the ported export job
config/
  .env.local.json                # per-ENV config (mirrors matchlibrary-baas src/config)
  .env.dev.json
  .env.stage.json
  .env.prod.json
stdLib/
  env.js                         # loads config/.env.<ENV>.json into process.env
  config.js                      # reads process.env → typed config object
  secret_manager_resources.js    # GCP Secret Manager (creds for Cloud SQL)
  aurora_resources.js            # Cloud SQL (pg): matches / tenant_mapping / catalog
  firestore_resources.js         # company-code library + competitor mapping
  gcs_resources.js               # upload generated files to GCS
  node_utils.js                  # CSV writing + staging dir management
  sendSlackNotification.js       # Slack helpers (mirrors PmtScheduleProcessing)
Dockerfile
deploy/deploy_jobs.sh            # build image + create/update every job & schedule
.github/workflows/
  ci.yml                         # install + ESM syntax check on PRs / develop
  deploy.yml                     # manual deploy to dev/prod via GCP auth
```

## Config & secrets

Config follows `matchlibrary-baas`: per-environment JSON files in `config/`, loaded into
`process.env` by `stdLib/env.js` (via `dotenv-json-complex`) and read by `stdLib/config.js`.

- `ENV` selects the file: `config/.env.<local|dev|stage|prod>.json`. Defaults to `local`.
  `CONFIG_OVERIDE=TRUE` forces `prod`.
- Each top-level key becomes a `process.env` entry; nested objects (`secretsManager`,
  `logger`) are stored as JSON strings (parsed where consumed).
- **Existing `process.env` vars always win** over the JSON file — so the env vars the deploy
  script sets on each Cloud Run Job take precedence over the committed defaults.
- The JSON files hold only **non-secret** values and Secret Manager secret **names**
  (`secretsManager.pmtAurora`). The actual Cloud SQL credentials live in GCP Secret Manager
  and are fetched at runtime by `stdLib/secret_manager_resources.js` — never committed.

```bash
npm install
ENV=local npm run match-library-gcs-export   # uses config/.env.local.json
```

GCP auth: Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS` locally; the
Cloud Run service account in deployment). The named secret's payload is JSON:
`{ "aurora": { host, username, password, database, port } }`.

## Deploy

### Via GitHub Actions (recommended)
The **Deploy** workflow (`.github/workflows/deploy.yml`) runs `workflow_dispatch` — pick
`dev` or `prod`. Configure these per GitHub **Environment** (Settings → Environments):

| Variable | Example |
|---|---|
| `GCP_WIF_PROVIDER` | `projects/123/locations/global/workloadIdentityPools/gh/providers/gh` |
| `GCP_DEPLOY_SA` | `gh-deployer@saas-dev-468820.iam.gserviceaccount.com` |
| `GCP_PROJECT` | `saas-dev-468820` |
| `GCP_REGION` | `us-central1` |
| `GCS_BUCKET` | `bungee-match-library-export-dev` |
| `DB_ENV` | `dev` |
| `FIRESTORE_DB` | `matchlibrary-baas` |
| `SECRETS_MANAGER` | `{"pmtAurora":"pmt-aurora-secret"}` |
| `RUN_SA` | `batch-jobs-sa@saas-dev-468820.iam.gserviceaccount.com` |
| `SCHED_SA` | `scheduler-sa@saas-dev-468820.iam.gserviceaccount.com` |

### Via CLI
```bash
PROJECT=saas-dev-468820 REGION=us-central1 ENV=dev \
GCS_BUCKET=bungee-match-library-export-dev \
DB_ENV=dev FIRESTORE_DB=matchlibrary-baas \
SECRETS_MANAGER='{"pmtAurora":"pmt-aurora-secret"}' \
./deploy/deploy_jobs.sh
```

### Prerequisites (one-time)
- A destination GCS bucket.
- Service accounts:
  - **run SA** (`RUN_SA`) — attached to the jobs; needs `roles/storage.objectAdmin` on the
    GCS bucket, `roles/secretmanager.secretAccessor` on the Cloud SQL secret, `roles/datastore.user`
    (Firestore read), and network access to Cloud SQL.
  - **scheduler SA** (`SCHED_SA`) — `roles/run.invoker`.
  - **deploy SA** (`GCP_DEPLOY_SA`) — Cloud Build, Cloud Run, Cloud Scheduler admin.
- Cloud SQL connection secret in Secret Manager (JSON `{ "aurora": { host, username, password, database, port } }`).

## Add another job

1. Write `jobs/<your_job>.js` exporting `export async function run(env)`.
2. Add an entry to `jobs/registry.js`.
3. Re-run the deploy workflow / script.
