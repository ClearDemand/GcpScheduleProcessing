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
# Cloud Scheduler isn't available in every Cloud Run region (e.g. us-east5), so
# the trigger location is decoupled from the job's REGION. A scheduler in any
# supported region can invoke a Cloud Run Job in REGION via its OAuth HTTP call.
SCHED_REGION="${SCHED_REGION:-us-central1}"
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

# ----------------------------------------------------------------------------
#  IAM bootstrap (idempotent). Creates the run/scheduler SAs if missing and
#  grants every role the deploy + runtime need, so a brand-new environment is
#  self-provisioning. Re-running is a no-op (add-iam-policy-binding is
#  idempotent). Set SETUP_IAM=false to skip (e.g. if the deployer lacks IAM
#  admin and an admin has already run this once).
#
#  The deploying identity must itself be allowed to create SAs and set IAM
#  policies for this to succeed; failures are warned (not fatal) so a deploy by
#  a limited deployer still proceeds against already-provisioned IAM.
# ----------------------------------------------------------------------------
# The identity that runs this deploy needs iam.serviceAccounts.actAs on the
# run/scheduler SAs. Default to the active gcloud account (the GHA SA in CI,
# your user locally); override with DEPLOYER=.
DEPLOYER="${DEPLOYER:-$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null | head -1)}"
# IAM members are "serviceAccount:" for SAs, "user:" for human accounts.
case "$DEPLOYER" in
    *gserviceaccount.com) DEPLOYER_MEMBER="serviceAccount:${DEPLOYER}" ;;
    *)                    DEPLOYER_MEMBER="user:${DEPLOYER}" ;;
esac

ensure_sa() {  # email, display-name
    local SA="$1" NAME
    NAME="${SA%%@*}"
    if gcloud iam service-accounts describe "$SA" --project "$PROJECT" >/dev/null 2>&1; then
        echo "    SA exists: $SA"
    else
        echo "    creating SA: $SA"
        gcloud iam service-accounts create "$NAME" --project "$PROJECT" \
            --display-name "$2" || echo "    WARNING: could not create $SA"
    fi
}

grant() {  # human label, then the full gcloud command as remaining args
    local LABEL="$1"; shift
    if "$@" >/dev/null 2>&1; then
        echo "    granted: $LABEL"
    else
        echo "    WARNING: failed to grant $LABEL (deployer may lack IAM admin; grant manually)"
    fi
}

provision_iam() {
    echo "==> Ensuring service accounts + IAM (idempotent) for ${ENV} / ${PROJECT}"
    echo "    deployer = ${DEPLOYER_MEMBER}"
    ensure_sa "$RUN_SA"   "Cloud Run Jobs runtime SA (GcpScheduleProcessing)"
    ensure_sa "$SCHED_SA" "Cloud Scheduler invoker SA (GcpScheduleProcessing)"

    # Deployer must be able to actAs both SAs (sets --service-account /
    # --oauth-service-account-email on the resources below).
    for SA in "$RUN_SA" "$SCHED_SA"; do
        grant "actAs $SA" \
            gcloud iam service-accounts add-iam-policy-binding "$SA" \
                --project "$PROJECT" --member="$DEPLOYER_MEMBER" \
                --role="roles/iam.serviceAccountUser"
    done

    # Run SA: write to the destination bucket.
    grant "storage.objectAdmin on gs://${GCS_BUCKET}" \
        gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
            --member="serviceAccount:${RUN_SA}" --role="roles/storage.objectAdmin"

    # Run SA: read every secret named in config/.env.<ENV>.json secretsManager.
    local SECRET_NAMES
    SECRET_NAMES="$(node -p "Object.values(require('./config/.env.${ENV}.json').secretsManager||{}).join('\n')")"
    while IFS= read -r SECRET; do
        [ -z "$SECRET" ] && continue
        grant "secretAccessor on ${SECRET}" \
            gcloud secrets add-iam-policy-binding "$SECRET" --project "$PROJECT" \
                --member="serviceAccount:${RUN_SA}" --role="roles/secretmanager.secretAccessor"
    done <<< "$SECRET_NAMES"

    # Run SA: Firestore + Cloud SQL (no resource-scoped option for these).
    grant "datastore.user (Firestore)" \
        gcloud projects add-iam-policy-binding "$PROJECT" \
            --member="serviceAccount:${RUN_SA}" --role="roles/datastore.user" --condition=None
    grant "cloudsql.client (Cloud SQL)" \
        gcloud projects add-iam-policy-binding "$PROJECT" \
            --member="serviceAccount:${RUN_SA}" --role="roles/cloudsql.client" --condition=None

    # Scheduler SA: invoke the Cloud Run Jobs it triggers.
    grant "run.invoker (scheduler)" \
        gcloud projects add-iam-policy-binding "$PROJECT" \
            --member="serviceAccount:${SCHED_SA}" --role="roles/run.invoker" --condition=None

    # Run SA: sign its own GCS V4 signed URLs (report download links). Cloud
    # Run's keyless ADC signs via the IAM Credentials signBlob API, which
    # requires the calling identity to hold this role on itself — without it,
    # getSignedUrl() fails at runtime with a permission error, not a deploy-time
    # one.
    grant "iam.serviceAccountTokenCreator (self, for GCS V4 signed URLs)" \
        gcloud iam service-accounts add-iam-policy-binding "$RUN_SA" \
            --project "$PROJECT" --member="serviceAccount:${RUN_SA}" \
            --role="roles/iam.serviceAccountTokenCreator"
}

if [ "${SETUP_IAM:-true}" = "true" ]; then
    provision_iam
    echo ""
fi

echo "==> Building image ${IMAGE}"
gcloud builds submit --project "$PROJECT" --tag "$IMAGE" .

# Emit one line per job from the registry: name|schedule|timezone|timeout|memory
# schedule/timezone default to "" for trigger-only jobs (no Cloud Scheduler
# entry); timeout defaults to 600s if a trigger-only job omits it.
JOBS="$(node --input-type=module -e '
import r from "./jobs/registry.js";
for (const [name, j] of Object.entries(r)) {
  console.log([name, j.schedule || "", j.timezone || "", j.timeout || "600s", j.memory || "512Mi"].join("|"));
}')"

while IFS='|' read -r NAME SCHEDULE TIMEZONE TIMEOUT MEMORY; do
    [ -z "$NAME" ] && continue
    echo ""
    echo "==> Deploying Cloud Run Job: ${NAME}"

    # Derive a Node heap cap from the container memory. V8 doesn't read the
    # cgroup limit, so without --max-old-space-size it caps old-space at ~2GB and
    # OOMs ("Ineffective mark-compacts near heap limit") well before the container
    # limit. Use ~75% of the container, leaving headroom for off-heap Buffers
    # (AdmZip ZIPs / readFileSync) and the runtime.
    case "$MEMORY" in
        *Gi) MEM_MB=$(( ${MEMORY%Gi} * 1024 )) ;;
        *Mi) MEM_MB=${MEMORY%Mi} ;;
        *)   MEM_MB=512 ;;
    esac
    HEAP_MB=$(( MEM_MB * 3 / 4 ))

    # Cloud Run requires >=2 vCPU for >4Gi of memory. Derive the minimum vCPU
    # that satisfies the requested memory so a job can ask for 8Gi via registry
    # without also having to hand-set CPU.
    if [ "$MEM_MB" -gt 4096 ]; then CPU=2; else CPU=1; fi

    # Use ^@^ delimiter so commas inside secretsManager JSON aren't split.
    # secretsManager is intentionally omitted so the per-env config/.env.<ENV>.json
    # supplies it; append it only when SECRETS_MANAGER is set as an override.
    ENV_VARS="^@^JOB_NAME=${NAME}@ENV=${ENV}@GCP_PROJECT_ID=${PROJECT}@FIRESTORE_DATABASE_ID=${FIRESTORE_DB}@dbENV=${DB_ENV}@gcs_bucket=${GCS_BUCKET}@NODE_OPTIONS=--max-old-space-size=${HEAP_MB}"
    if [ -n "$SECRETS_MANAGER" ]; then
        ENV_VARS="${ENV_VARS}@secretsManager=${SECRETS_MANAGER}"
    fi

    gcloud run jobs deploy "$NAME" \
        --project "$PROJECT" --region "$REGION" \
        --image "$IMAGE" \
        --task-timeout "$TIMEOUT" \
        --memory "$MEMORY" \
        --cpu "$CPU" \
        --max-retries 2 \
        --service-account "$RUN_SA" \
        --vpc-connector retailscape-vpc-connector \
        --vpc-egress private-ranges-only \
        --set-env-vars "$ENV_VARS"

    if [ -n "$SCHEDULE" ]; then
        echo "==> Scheduling trigger: trigger-${NAME} (${SCHEDULE} ${TIMEZONE})"
        RUN_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${NAME}:run"

        if gcloud scheduler jobs describe "trigger-${NAME}" --project "$PROJECT" --location "$SCHED_REGION" >/dev/null 2>&1; then
            gcloud scheduler jobs update http "trigger-${NAME}" \
                --project "$PROJECT" --location "$SCHED_REGION" \
                --schedule "$SCHEDULE" --time-zone "$TIMEZONE" \
                --uri "$RUN_URI" --http-method POST \
                --oauth-service-account-email "$SCHED_SA"
        else
            gcloud scheduler jobs create http "trigger-${NAME}" \
                --project "$PROJECT" --location "$SCHED_REGION" \
                --schedule "$SCHEDULE" --time-zone "$TIMEZONE" \
                --uri "$RUN_URI" --http-method POST \
                --oauth-service-account-email "$SCHED_SA"
        fi
    else
        echo "==> No schedule for ${NAME}; skipping Cloud Scheduler (trigger-only job)."
    fi

    # Trigger-only jobs need an external caller authorized to invoke *this*
    # job specifically. match-update-processor and
    # auto-ingestion-processor are both invoked by the same caller
    # (matchlibrary-baas's Cloud Function runtime SA), so they share one
    # invoker-SA variable. MATCHLIBRARY_BAAS_INVOKER_SA is unset by default
    # (safe: the job just stays uninvokable by anyone but existing job-runner
    # principals) until the calling service's account email is known.
    # MATCH_UPDATE_INVOKER_SA is accepted as a fallback for anyone who already
    # has it set from before auto-ingestion-processor existed.
    INVOKER_SA="${MATCHLIBRARY_BAAS_INVOKER_SA:-${MATCH_UPDATE_INVOKER_SA:-}}"
    case "$NAME" in
        match-update-processor|auto-ingestion-processor)
            if [ -n "$INVOKER_SA" ]; then
                grant "run.invoker (${INVOKER_SA}) on ${NAME}" \
                    gcloud run jobs add-iam-policy-binding "$NAME" \
                        --project "$PROJECT" --region "$REGION" \
                        --member="serviceAccount:${INVOKER_SA}" --role="roles/run.invoker"
            fi
            ;;
    esac
done <<< "$JOBS"

echo ""
echo "==> Done. Trigger a manual run with:"
echo "    gcloud run jobs execute <job-name> --project ${PROJECT} --region ${REGION}"
