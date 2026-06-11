// ----------------------------------------------------------------------------
//  Environment loader (ES module)
//  Mirrors matchlibrary-baas/src/lib/server/env.js.
//
//  Loads config/.env.<ENV>.json into process.env via dotenv-json-complex. Each
//  top-level key becomes a process.env entry; nested objects (e.g.
//  `secretsManager`, `logger`) are stored as JSON strings (parse with
//  JSON.parse where consumed). Existing process.env keys are NOT overwritten,
//  so Cloud Run job env vars always take precedence over the JSON file.
//
//  Cannot use the logger here — env must load first; use console.
// ----------------------------------------------------------------------------
import dotenv from 'dotenv-json-complex';

export class DotEnv {
    constructor() {
        process.env.ENV = process.env.ENV || 'dev';

        const result = dotenv({ directory: 'config/', environment: process.env.ENV });

        // dotenv-json-complex returns { parsed } on success, or an Error on a
        // missing/unreadable file (it does not throw). On Cloud Run the env
        // vars are set directly, so a missing JSON file is not fatal.
        if (result && result.parsed) {
            this.isLoaded = true;
            console.log(`Environment config: config/.env.${process.env.ENV}.json loaded.`);
        } else {
            this.isLoaded = false;
            console.log(`No config/.env.${process.env.ENV}.json found; using process.env only.`);
        }
    }
}

export default new DotEnv();
