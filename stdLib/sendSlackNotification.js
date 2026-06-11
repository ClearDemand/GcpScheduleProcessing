// ----------------------------------------------------------------------------
//  Slack notifications (ES module)
//  Trimmed mirror of PmtScheduleProcessing/stdLib/sendSlackNotification.js.
// ----------------------------------------------------------------------------
import moment from 'moment';
import path from 'path';
import axios from 'axios';

const scriptName = path.basename(new URL(import.meta.url).pathname);
const slackEndpoint = process.env.slack_endpoint;

const timestampBlock = () =>
    `${moment().utcOffset(0).format('YYYY/MM/DD HH:mm')} UTC\n`
    + `${moment().utcOffset('+05:30').format('YYYY/MM/DD HH:mm')} IST\n`
    + `${moment().utcOffset('-05:00').format('YYYY/MM/DD HH:mm')} EST\n`;

export async function notifySlack(message) {
    return post({ username: 'GCP Scheduled Jobs', text: message });
}

export async function notifySlackProcessStart(process_title, company_code = false) {
    return post({
        username: 'GCP Scheduled Jobs',
        blocks: [
            { type: 'divider' },
            { type: 'section', text: { type: 'mrkdwn', text: `*${process_title} [ STARTED ]*` } },
            { type: 'section', text: { type: 'mrkdwn',
                text: `${company_code ? `*Client:* ${company_code}\n` : ''}*Started at*\n${timestampBlock()}` } }
        ]
    });
}

export async function notifySlackProcessCompleted(process_title, company_code, message) {
    return post({
        username: 'GCP Scheduled Jobs',
        blocks: [
            { type: 'divider' },
            { type: 'section', text: { type: 'mrkdwn', text: `*${process_title} [ COMPLETED ]*` } },
            { type: 'section', fields: [
                { type: 'mrkdwn', text: `*Completed at*\n${timestampBlock()}` },
                { type: 'mrkdwn', text: `${message}` }
            ] }
        ]
    });
}

export async function notifySlackStatus(process_title, status, company_code, message, force_notify = false) {
    return post({
        username: 'GCP Scheduled Jobs',
        blocks: [
            { type: 'divider' },
            { type: 'section', text: { type: 'mrkdwn', text: `*${process_title} [ ${status} ]*` } },
            { type: 'section', text: { type: 'mrkdwn', text: `${force_notify ? '<!here> ' : ''}${message}` } }
        ]
    });
}

async function post(data) {
    try {
        if (!slackEndpoint) { return false; }
        const resp = await axios({
            url: slackEndpoint,
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            data
        });
        return resp;
    } catch (error) {
        console.log(`${moment().format()} - ${scriptName} error in post: ${error.message}`);
        return false;
    }
}
