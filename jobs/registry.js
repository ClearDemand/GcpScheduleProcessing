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
//    schedule    - cron expression for Cloud Scheduler
//    timezone    - IANA tz for the schedule
//    timeout     - Cloud Run Job task timeout (e.g. '1800s' = 30m)
//    memory      - Cloud Run Job memory
//    env         - per-job env passed to run(); also set on the Cloud Run Job
// ----------------------------------------------------------------------------
export default {
    'match-library-gcs-export': {
        handlerPath: './jobs/match_library_gcs_export.js',
        // The S3 export (PmtScheduleProcessing) runs ~01:30 IST. Run after it so
        // the day's files exist. 04:00 IST gives a comfortable buffer.
        // Original AWS cron was 01:30 IST daily. Keep the same time.
        schedule: '30 1 * * *',
        timezone: 'Asia/Kolkata',
        timeout: '1800s',
        // Export loads matches → builds CSVs → ZIPs entirely in memory; 1Gi OOMs.
        // 4Gi gives headroom and stays within the 1-vCPU limit (>4Gi needs ≥2 vCPU).
        memory: '4Gi',
        env: {}
    }
};
