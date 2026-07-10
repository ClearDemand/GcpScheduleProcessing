// ----------------------------------------------------------------------------
//  Job registry — single source of truth for every scheduled job.
//
//  To add a new job:
//    1. Write jobs/<your_job>.js exporting `export async function run(env)`.
//    2. Add an entry here.
//    3. Re-run deploy/deploy_jobs.sh (or the GitHub "Deploy" workflow), which
//       creates/updates the Cloud Run Job + Cloud Scheduler trigger per entry.
//
//  Fields:
//    handlerPath - ES module exporting run(env), relative to repo root (with .js)
//    schedule    - cron expression for Cloud Scheduler. Omit for a trigger-only
//                  job (see triggerOnly below) — do not just leave it out by
//                  accident, a cron job with a missing schedule should fail
//                  loudly rather than silently deploy without a trigger.
//    timezone    - IANA tz for the schedule (only meaningful with `schedule`)
//    triggerOnly - true for jobs with no Cloud Scheduler trigger at all; they
//                  still deploy as a Cloud Run Job, invoked on demand by
//                  another service via the Cloud Run Admin API (IAM-gated,
//                  see deploy_jobs.sh's MATCH_UPDATE_INVOKER_SA-style grants).
//    timeout     - Cloud Run Job task timeout (e.g. '1800s' = 30m)
//    memory      - Cloud Run Job memory
//    env         - per-job env passed to run(); also set on the Cloud Run Job
// ----------------------------------------------------------------------------
export default {
    'match-library-gcs-export': {
        handlerPath: './jobs/match_library_gcs_export.js',
        // The S3 export (PmtScheduleProcessing) runs ~01:30 IST. Run after it so
        // the day's files exist. 05:30 IST gives a comfortable buffer.
        schedule: '30 5 * * *',
        timezone: 'Asia/Kolkata',
        timeout: '1800s',
        // Export loads matches → builds CSVs → ZIPs entirely in memory; 1Gi OOMs.
        // 4Gi OOM-killed (SIGKILL) on a large tenant, so bump to 8Gi. >4Gi needs
        // ≥2 vCPU, which deploy_jobs.sh derives automatically from this value.
        memory: '8Gi',
        env: {}
    },
    'match-update-processor': {
        handlerPath: './jobs/match_update_processor.js',
        // No schedule — triggered on demand (via the Cloud Run Admin API's
        // jobs.run) by whatever service inserts a row into matches_update_queue.
        triggerOnly: true,
        timeout: '1800s',
        memory: '1Gi',
        env: {}
    }
};
