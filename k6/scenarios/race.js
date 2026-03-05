/**
 * Race Test - 동시 주문 경쟁 (핵심 시나리오)
 *
 * 목적: N명이 동시에 한정판 재고를 주문할 때 Oversell 없이 정확히 재고만큼만 처리되는지 검증
 *
 * 시나리오:
 *   - 사용자 200명이 동시에 재고 100개짜리 상품을 주문
 *   - 기대 결과: 성공 100건, 품절 100건
 *   - Oversell(성공 > 재고)이 없어야 함
 *
 * 실행: k6 run -e PRODUCT_ID=1 -e STOCK=100 k6/scenarios/race.js
 */

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8888';
const PRODUCT_ID = parseInt(__ENV.PRODUCT_ID || '1');
const STOCK = parseInt(__ENV.STOCK || '10000');
const USER_COUNT = parseInt(__ENV.USER_COUNT || '500');

// 커스텀 메트릭
const orderSuccess = new Counter('order_success');
const orderSoldOut = new Counter('order_sold_out');
const orderServerError = new Counter('order_server_error');
const oversellRate = new Rate('oversell_occurred');

export const options = {
    scenarios: {
        // shared-iterations: N명이 정확히 1번씩 요청 - 동시성 시뮬레이션에 적합
        race: {
            executor: 'shared-iterations',
            vus: USER_COUNT,
            iterations: USER_COUNT,
            maxDuration: '1m',
        },
    },
    thresholds: {
        // 서버 에러 0건
        order_server_error: ['count==0'],
        // 성공 건수가 재고를 초과하면 Oversell - 반드시 통과해야 함
        order_success: [`count<=${STOCK}`],
        // p95 응답시간 2초 이내
        http_req_duration: ['p(95)<2000'],
    },
};

export default function () {
    // __VU: 1 ~ USER_COUNT (각 VU가 고유한 userId 사용)
    const userId = __VU;

    const payload = JSON.stringify({
        userId: userId,
        productId: PRODUCT_ID,
        quantity: 1,
    });

    const res = http.post(`${BASE_URL}/api/v1/orders`, payload, {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: 'create_order' },
    });

    check(res, {
        '5xx 없음': (r) => r.status < 500,
    });

    if (res.status === 200) {
        orderSuccess.add(1);
        // 성공 건수가 재고 초과 시 Oversell 플래그
        oversellRate.add(orderSuccess.name > STOCK ? 1 : 0);
    } else if (res.status === 400) {
        orderSoldOut.add(1);
    } else {
        orderServerError.add(1);
        console.error(`[VU ${__VU}] 예상치 못한 응답: ${res.status} - ${res.body}`);
    }
}

export function handleSummary(data) {
    const success = data.metrics['order_success']?.values?.count ?? 0;
    const soldOut = data.metrics['order_sold_out']?.values?.count ?? 0;
    const serverError = data.metrics['order_server_error']?.values?.count ?? 0;

    const result = [
        '=== 동시 주문 경쟁 결과 ===',
        `총 요청     : ${USER_COUNT}건`,
        `주문 성공   : ${success}건  (기대: ${STOCK}건)`,
        `품절 처리   : ${soldOut}건`,
        `서버 에러   : ${serverError}건`,
        `Oversell    : ${success > STOCK ? '❌ 발생 (' + (success - STOCK) + '건 초과)' : '✅ 없음'}`,
        '',
    ].join('\n');

    console.log('\n' + result);

    return {
        stdout: result,
    };
}
