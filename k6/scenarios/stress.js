/**
 * Stress Test - 점진적 부하 증가
 *
 * 목적: 부하가 증가할수록 응답시간과 에러율이 어떻게 변하는지 관찰
 *       시스템이 버틸 수 있는 임계 TPS와 VU 수를 파악
 *
 * 단계:
 *   0 →  20 VU (30s) : 워밍업
 *  20 →  50 VU (60s) : 정상 부하
 *  50 → 100 VU (60s) : 높은 부하
 * 100 → 150 VU (60s) : 임계점 탐색
 * 150 →   0 VU (30s) : 복구 확인
 *
 * 참고: 재고가 소진되면 이후 요청은 모두 400(품절) 반환
 *       이 테스트는 처리량(TPS)과 응답시간 측정이 목적
 *
 * 실행: k6 run -e PRODUCT_ID=1 k6/scenarios/stress.js
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
        { duration: '30s', target: 50  },  // 워밍업
        { duration: '60s', target: 150 },  // 정상 부하
        { duration: '60s', target: 300 },  // 높은 부하
        { duration: '60s', target: 500 },  // 임계점
        { duration: '30s', target: 0   },  // 복구
    ],
    thresholds: {
        // 커넥션 타임아웃 포함 실패율 10% 미만
        http_req_failed: ['rate<0.10'],
        // p95 2초, p99 5초 미만
        http_req_duration: ['p(95)<2000', 'p(99)<5000'],
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

    // 실제 사용자처럼 요청 사이 짧은 대기 (0.1 ~ 0.5초)
    sleep(Math.random() * 0.4 + 0.1);
}
