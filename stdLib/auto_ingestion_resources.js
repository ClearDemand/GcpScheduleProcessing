// ----------------------------------------------------------------------------
//  Auto ingestion trigger-queue resources (ES module)
//
//  Queue claim/status for auto_ingestion_trigger_queue, populated by
//  matchlibrary-baas's POST /matches/auto-ingestion/trigger (one row per
//  client, not one row per whole trigger call -- see that endpoint's
//  insertAutoIngestionTriggerRows/triggerAutoIngestionProcessor for why).
//
//  Simpler than match_update_resources.js's claimNextPendingGroup: this claims
//  a single row (one client = one unit of work), not a process_id-grouped set,
//  since there's nothing to group -- each row is already a complete unit.
// ----------------------------------------------------------------------------
import * as aurora from './aurora_resources.js';

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// Atomically claims the oldest pending row and flips it to 'processing'.
// FOR UPDATE SKIP LOCKED lets a losing claimant move on instead of blocking,
// mirroring claimNextPendingGroup's retry-until-confirmed-empty shape.
export async function claimNextPendingClient(maxAttempts = 5) {
    const claimQuery = `
        UPDATE auto_ingestion_trigger_queue
        SET ingestion_status = 'processing', updated_timestamp = NOW()
        WHERE id = (
            SELECT id FROM auto_ingestion_trigger_queue
            WHERE ingestion_status = 'pending'
            ORDER BY created_timestamp ASC LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        AND ingestion_status = 'pending'
        RETURNING *;
    `;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        const rows = await aurora.runQuery(claimQuery);
        if (rows && rows.length > 0) return rows[0];

        const remaining = await aurora.runQuery(
            `SELECT 1 FROM auto_ingestion_trigger_queue WHERE ingestion_status = 'pending' LIMIT 1`
        );
        if (!remaining || remaining.length === 0) return null;
        await sleep(75 * attempt);
    }
    return null;
}

export async function markClientStatus(id, status, statusComment) {
    return aurora.runQuery(
        `UPDATE auto_ingestion_trigger_queue
         SET ingestion_status = $2, status_comments = $3, updated_timestamp = NOW()
         WHERE id = $1
         RETURNING *;`,
        [id, status, statusComment]
    );
}
