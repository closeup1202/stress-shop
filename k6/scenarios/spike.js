/**
 * Spike Test - 단일 트래픽 폭증
 *
 * 목적: 평상시 트래픽에서 갑작스럽게 폭증한 뒤 복구되는 시나리오에서
 *       시스템이 오류 없이 버티고 정상 상태로 돌아오는지 확인
 *
 * 단계:
 *   0 →  20 VU (20s) : 평상시
 *   20 → 500 VU ( 5s) : 급격한 폭증 (스파이크)
 *   500 VU       (30s) : 폭증 유지
 *   500 →  20 VU ( 5s) : 급격한 축소
 *   20 →   0 VU (20s) : 복구 확인
 *
 * 참고: 스파이크 구간(5s) 동안 VU가 0 → 500으로 급격히 오르는 것이 핵심
 *       복구 후 에러율과 응답시간이 정상으로 돌아오는지 관찰
 *
 * 실행: docker compose run --rm -e PRODUCT_ID=$PRODUCT_ID k6 run /k6/scenarios/spike.js
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// 200(성공), 400(품절)은 정상 응답으로 간주 - http_req_failed에서 제외
http.setResponseCallback(http.expectedStatuses(200, 400));

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8888';
const PRODUCT_ID = parseInt(__ENV.PRODUCT_ID || '1');

const orderSuccess = new Counter('order_success');
const orderSoldOut = new Counter('order_sold_out');
const orderServerError = new Counter('order_server_error');
const orderDuration = new Trend('order_duration', true);

export const options = {
    stages: [
        { duration: '20s', target: 20  },  // 평상시
        { duration: '5s',  target: 500 },  // 급격한 폭증
        { duration: '30s', target: 500 },  // 폭증 유지
        { duration: '5s',  target: 20  },  // 급격한 축소
        { duration: '20s', target: 0   },  // 복구 확인
    ],
    thresholds: {
        // p95 3초, p99 10초 미만 (스파이크 구간 감안)
        http_req_duration: ['p(95)<3000', 'p(99)<10000'],
    },
};

// VU별 고유 userId 풀 (1 ~ 500)
function getUserId() {
    return ((__VU - 1) % 500) + 1;
}

export default function () {
    const userId = getUserId();

    const payload = JSON.stringify({
        userId: userId,
        productId: PRODUCT_ID,
        quantity: 1,
    });

    const res = http.post(`${BASE_URL}/api/v1/orders`, payload, {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: 'create_order' },
    });

    orderDuration.add(res.timings.duration);

    check(res, {
        '5xx 없음': (r) => r.status < 500,
    });

    if (res.status === 200) {
        orderSuccess.add(1);
    } else if (res.status === 400) {
        orderSoldOut.add(1);
    } else {
        orderServerError.add(1);
    }

    sleep(Math.random() * 0.4 + 0.1);
}
