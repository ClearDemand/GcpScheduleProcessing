// ----------------------------------------------------------------------------
//  SES SMTP email resources (ES module) — report-delivery emails for the
//  match-update-processor job.
//
//  Same approach as matchlibrary-baas/src/lib/notification/EmailNotification.js:
//  nodemailer SMTP transport, credentials read off the `emailProvider` block
//  of the same GCP Secret Manager secret aurora_resources.js already uses
//  (secretsManager.pmtAurora) rather than a separate secret.
// ----------------------------------------------------------------------------
import nodemailer from 'nodemailer';
import { getSecretAsJson } from './secret_manager_resources.js';

const ALLOWED_EMAIL_DOMAINS = ['bungeetech.com', 'cleardemand.com'];
const FROM_EMAIL = 'bungeeinternal@bungeetech.com';

let transporter;

export async function init() {
    const secretName = JSON.parse(process.env.secretsManager).pmtAurora;
    const secret = await getSecretAsJson(secretName);
    const sesConfig = secret.emailProvider;

    transporter = nodemailer.createTransport({
        host: sesConfig.endpoint,
        port: parseInt(sesConfig.port, 10),
        auth: { user: sesConfig.username, pass: sesConfig.password },
        secureConnection: true,
        tls: { ciphers: sesConfig.ciphers }
    });
    console.log(`SES SMTP transport ready (host=${sesConfig.endpoint})`);
}

function isAllowedRecipient(email) {
    const lower = `${email || ''}`.toLowerCase();
    return ALLOWED_EMAIL_DOMAINS.some(d => lower.includes(d));
}

// Sends the report email only to allowlisted internal recipients (mirrors the
// old poll.js gate on userEmail) — no-ops for anyone else instead of the job
// having to check the domain itself.
export async function sendReportEmail(to, subject, html) {
    if (!isAllowedRecipient(to)) {
        console.log(`sendReportEmail: recipient "${to}" not in allowlist (${ALLOWED_EMAIL_DOMAINS.join(', ')}); skipping`);
        return false;
    }
    try {
        await transporter.sendMail({ from: FROM_EMAIL, to, replyTo: FROM_EMAIL, subject, html });
        console.log(`sendReportEmail: sent to ${to}`);
        return true;
    } catch (err) {
        console.log(`sendReportEmail err: ${err.message}`);
        return false;
    }
}
