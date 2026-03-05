/**
 * Smoke Test - 기본 동작 확인
 *
 * 목적: 배포 직후 또는 부하 테스트 전 시스템이 정상 동작하는지 최소 검증
 * VU : 1
 * 시간: 30초
 *
 * 실행: k6 run -e PRODUCT_ID=1 k6/scenarios/smoke.js
 */

import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8888';
const PRODUCT_ID = parseInt(__ENV.PRODUCT_ID || '1');

export const options = {
    vus: 1,
    duration: '30s',
    thresholds: {
        http_req_duration: ['p(95)<1000'],
        http_req_failed: ['rate<0.01'],
    },
};

export default function () {
    const payload = JSON.stringify({
        userId: __VU,
        productId: PRODUCT_ID,
        quantity: 1,
    });

    const res = http.post(`${BASE_URL}/api/v1/orders`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    // 200(성공) 또는 400(품절/잔액 부족)은 정상 응답으로 간주
    check(res, {
        'status is 2xx or 4xx': (r) => r.status === 200 || r.status === 400,
        '5xx 없음': (r) => r.status < 500,
        'response time < 1s': (r) => r.timings.duration < 1000,
    });

    sleep(1);
}
