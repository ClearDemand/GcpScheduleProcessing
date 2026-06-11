#!/bin/bash
# ----------------------------------------------------------------------------
#  Build the shared image once, then create/update a Cloud Run Job + Cloud
#  Scheduler trigger for every entry in jobs/registry.js.
#
#  Usage:
#    PROJECT=saas-dev-468820 REGION=us-central1 ENV=dev \
#    GCS_BUCKET=bungee-match-library-export-dev \
#    DB_ENV=dev FIRESTORE_DB=matchlibrary-baas \
#    ./deploy/deploy_jobs.sh
#
#  secretsManager (the per-env map of secret names, incl. rsAurora) is NOT set
#  here — it comes from the committed config/.env.<ENV>.json so each environment
#  uses its OWN secrets (dev/retailscape/psql, prod/retailscape/psql, ...).
#  Set SECRETS_MANAGER only to override that config value for a one-off deploy.
# ----------------------------------------------------------------------------
set -euo pipefail

PROJECT="${PROJECT:?set PROJECT (gcp project id)}"
REGION="${REGION:-us-central1}"
ENV="${ENV:-dev}"
GCS_BUCKET="${GCS_BUCKET:?set GCS_BUCKET (destination bucket)}"
DB_ENV="${DB_ENV:-$ENV}"                            # Postgres search_path / schema
FIRESTORE_DB="${FIRESTORE_DB:-matchlibrary-baas}"   # Firestore database id
SECRETS_MANAGER="${SECRETS_MANAGER:-}"              # optional override; default: per-env config/.env.<ENV>.json
RUN_SA="${RUN_SA:-batch-jobs-sa@${PROJECT}.iam.gserviceaccount.com}"
SCHED_SA="${SCHED_SA:-scheduler-sa@${PROJECT}.iam.gserviceaccount.com}"
IMAGE="gcr.io/${PROJECT}/gcp-schedule-processing:latest"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Building image ${IMAGE}"
gcloud builds submit --project "$PROJECT" --tag "$IMAGE" .

# Emit one line per job from the registry: name|schedule|timezone|timeout|memory
JOBS="$(node --input-type=module -e '
import r from "./jobs/registry.js";
for (const [name, j] of Object.entries(r)) {
  console.log([name, j.schedule, j.timezone, j.timeout, j.memory || "512Mi"].join("|"));
}')"

while IFS='|' read -r NAME SCHEDULE TIMEZONE TIMEOUT MEMORY; do
    [ -z "$NAME" ] && continue
    echo ""
    echo "==> Deploying Cloud Run Job: ${NAME}"

    # Use ^@^ delimiter so commas inside secretsManager JSON aren't split.
    # secretsManager is intentionally omitted so the per-env config/.env.<ENV>.json
    # supplies it; append it only when SECRETS_MANAGER is set as an override.
    ENV_VARS="^@^JOB_NAME=${NAME}@ENV=${ENV}@GCP_PROJECT_ID=${PROJECT}@FIRESTORE_DATABASE_ID=${FIRESTORE_DB}@dbENV=${DB_ENV}@gcs_bucket=${GCS_BUCKET}"
    if [ -n "$SECRETS_MANAGER" ]; then
        ENV_VARS="${ENV_VARS}@secretsManager=${SECRETS_MANAGER}"
    fi

    gcloud run jobs deploy "$NAME" \
        --project "$PROJECT" --region "$REGION" \
        --image "$IMAGE" \
        --task-timeout "$TIMEOUT" \
        --memory "$MEMORY" \
        --max-retries 2 \
        --service-account "$RUN_SA" \
        --set-env-vars "$ENV_VARS"

    echo "==> Scheduling trigger: trigger-${NAME} (${SCHEDULE} ${TIMEZONE})"
    RUN_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${NAME}:run"

    if gcloud scheduler jobs describe "trigger-${NAME}" --project "$PROJECT" --location "$REGION" >/dev/null 2>&1; then
        gcloud scheduler jobs update http "trigger-${NAME}" \
            --project "$PROJECT" --location "$REGION" \
            --schedule "$SCHEDULE" --time-zone "$TIMEZONE" \
            --uri "$RUN_URI" --http-method POST \
            --oauth-service-account-email "$SCHED_SA"
    else
        gcloud scheduler jobs create http "trigger-${NAME}" \
            --project "$PROJECT" --location "$REGION" \
            --schedule "$SCHEDULE" --time-zone "$TIMEZONE" \
            --uri "$RUN_URI" --http-method POST \
            --oauth-service-account-email "$SCHED_SA"
    fi
done <<< "$JOBS"

echo ""
echo "==> Done. Trigger a manual run with:"
echo "    gcloud run jobs execute <job-name> --project ${PROJECT} --region ${REGION}"
