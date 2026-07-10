// ----------------------------------------------------------------------------
//  Entry point — Cloud Run Job dispatcher (ES module).
//
//  Run-to-completion: runs ONE job (selected by JOB_NAME), then exits 0
//  (success) or 1 (failure). Cloud Scheduler triggers the Cloud Run Job on a
//  cron; Cloud Run reads the exit code.
// ----------------------------------------------------------------------------
import './stdLib/env.js'; // loads config/.env.<ENV>.json into process.env (must run first)
import moment from 'moment';
import registry from './jobs/registry.js';

async function main() {
    const jobName = process.env.JOB_NAME;
    const job = registry[jobName];

    if (!job) {
        console.error(`Unknown JOB_NAME "${jobName}". Available jobs: ${Object.keys(registry).join(', ')}`);
        process.exit(1);
    }

    try {
        console.log(`${moment().format()} Starting job [${jobName}] ENV=${process.env.ENV || 'dev'}`);

        const handler = await import(job.handlerPath);
        await handler.run(job.env || {});

        console.log(`${moment().format()} Job [${jobName}] completed successfully`);
        process.exit(0);
    } catch (err) {
        console.error(`${moment().format()} Job [${jobName}] failed: ${err && err.stack ? err.stack : err}`);
        process.exit(1);
    }
}

main();
