import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    vus: 50,
    duration: '60s',
};

export default function () {
        const loginRes = http.post(
        'http://localhost:8080/auth/token',
        JSON.stringify({
            client_id:"media",
            client_secret:"fLgmkTdhcst0bsJFFeztDlV7/fapJ+SUZz+5Jj49h/Y=",
        }),
        {
            headers: {
                'Content-Type': 'application/json',
            },
        }
    );
    check(loginRes, {
        'login successful': (r) => r.status === 200,
    });
    const token = loginRes.json('accessToken');

    const dataRes = http.get(
        'http://localhost:8080/aggregate/hazards',
        {
            headers: {
                Authorization: `Bearer ${token}`,
            },
        }
    );
    const dataOK =check(dataRes, {
        'data request successful': (r) => r.status === 200,
    });

    if (!dataOK) {
        console.log(
            `DATA FAILED: status=${dataRes.status}, body=${dataRes.body}`
        );
    }
    sleep(1);
}