import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URLS = (__ENV.BASE_URLS || __ENV.BASE_URL || [
    'http://100.73.41.16:30080',
    'http://100.92.240.6:30080',
    'http://100.89.152.24:30080',
].join(',')).split(',').map((u) => u.trim()).filter(Boolean);

const CLIENT_ID = __ENV.CLIENT_ID || 'media';
const CLIENT_SECRET = __ENV.CLIENT_SECRET || 'fLgmkTdhcst0bsJFFeztDlV7/fapJ+SUZz+5Jj49h/Y=';
const TARGET_VUS = parseInt(__ENV.TARGET_VUS || '10000', 10);
const DURATION = __ENV.DURATION || '120s';

export const options = {
    vus: TARGET_VUS,
    duration: DURATION,
};

// Per-VU credentials (never share refresh_token across VUs because refresh_token is single-use)
let vuAccessToken = null;
let vuRefreshToken = null;
let vuExpiresAt = 0;
let vuNextAuthAttemptAt = 0;

function parseTokenPair(res) {
    if (res.status !== 200) {
        return null;
    }
    const accessToken = res.json('accessToken');
    const refreshToken = res.json('refreshToken');
    const expiresInSec = Number(res.json('expiresIn') || 60);
    if (!accessToken || !refreshToken) {
        return null;
    }
    return {
        accessToken,
        refreshToken,
        expiresAt: Date.now() + expiresInSec * 1000,
    };
}

function loginWithCredentials(baseUrl) {
    const res = http.post(
        `${baseUrl}/auth/token`,
        JSON.stringify({
            client_id: CLIENT_ID,
            client_secret: CLIENT_SECRET,
        }),
        {
            headers: { 'Content-Type': 'application/json' },
            timeout: '20s',
            tags: { endpoint: 'auth_token' },
        }
    );
    const pair = parseTokenPair(res);
    if (!pair) {
        console.log(`LOGIN FAILED (${baseUrl}): status=${res.status}, error=${res.error}, body=${res.body}`);
        return null;
    }
    return pair;
}

function refreshOrLogin(baseUrl) {
    if (vuRefreshToken) {
        const res = http.post(
            `${baseUrl}/auth/refresh`,
            JSON.stringify({
                refresh_token: vuRefreshToken,
            }),
            {
                headers: { 'Content-Type': 'application/json' },
                timeout: '20s',
                tags: { endpoint: 'auth_refresh' },
            }
        );
        const pair = parseTokenPair(res);
        if (pair) {
            vuAccessToken = pair.accessToken;
            vuRefreshToken = pair.refreshToken;
            vuExpiresAt = pair.expiresAt;
            return true;
        }
        if (res.status !== 401) {
            console.log(`REFRESH FAILED (${baseUrl}): status=${res.status}, error=${res.error}, body=${res.body}`);
        }
    }
    const pair = loginWithCredentials(baseUrl);
    if (pair) {
        vuAccessToken = pair.accessToken;
        vuRefreshToken = pair.refreshToken;
        vuExpiresAt = pair.expiresAt;
        return true;
    }
    return false;
}

export function setup() {
    for (const url of BASE_URLS) {
        const pair = loginWithCredentials(url);
        if (pair) {
            return {
                accessToken: pair.accessToken,
                expiresAt: pair.expiresAt,
            };
        }
    }
    throw new Error('Setup login failed on all BASE_URLS');
}

export default function (data) {
    const baseUrl = BASE_URLS[(__VU - 1) % BASE_URLS.length];

    if (!vuAccessToken) {
        vuAccessToken = data.accessToken;
        vuExpiresAt = data.expiresAt;
    }

    // Stagger proactive refresh before the 60s JWT TTL expires
    const refreshLeadMs = vuRefreshToken
        ? 10000 + ((__VU % 8) * 500)
        : 8000 + ((__VU % 45) * 1000);
    if (Date.now() >= vuExpiresAt - refreshLeadMs && Date.now() >= vuNextAuthAttemptAt) {
        if (!refreshOrLogin(baseUrl)) {
            vuNextAuthAttemptAt = Date.now() + 2000 + Math.random() * 3000;
        }
    }

    let dataRes = http.get(
        `${baseUrl}/aggregate/hazards`,
        {
            headers: {
                Authorization: `Bearer ${vuAccessToken}`,
            },
            timeout: '30s',
            tags: { endpoint: 'aggregate_hazards' },
        }
    );

    if (dataRes.status === 401) {
        if (refreshOrLogin(baseUrl)) {
            dataRes = http.get(
                `${baseUrl}/aggregate/hazards`,
                {
                    headers: {
                        Authorization: `Bearer ${vuAccessToken}`,
                    },
                    timeout: '30s',
                    tags: { endpoint: 'aggregate_hazards' },
                }
            );
        }
    }

    const dataOK = check(dataRes, {
        'data request successful': (r) => r.status === 200,
    });

    if (!dataOK) {
        console.log(`DATA FAILED (${baseUrl}): status=${dataRes.status}, error=${dataRes.error}, body=${dataRes.body}`);
    }

    sleep(1);
}
