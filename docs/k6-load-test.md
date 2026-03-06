# Step 5: k6 부하 테스트

## 개요

Redis 선차감 + Kafka Outbox 패턴으로 구성된 주문 시스템의 정합성과 성능을 k6로 검증한다.

- **재고 정합성**: Oversell(초과 판매) 없이 정확히 재고만큼만 주문이 처리되는지
- **응답시간**: 고부하에서도 p95 < 2s를 유지하는지
- **처리량(TPS)**: VU 증가에 따른 처리량 변화
- **복구력**: 스파이크 이후 정상 상태로 돌아오는지

---

## 아키텍처 흐름

```
k6 → [POST /api/v1/orders]
        |
     Redis DECR (재고 선차감)  <-- 품절이면 즉시 400 반환
        |
     DB INSERT (Order + OutboxEvent) - 동기
        |
     [200 응답 반환]
        | (비동기)
     OutboxPoller → Kafka → OrderEventConsumer
                              |
                           지갑 차감 + Order 상태 COMPLETED
```

HTTP 응답은 Redis + DB INSERT까지만 하고 즉시 반환하기 때문에 응답시간이 매우 빠르다.

---

## 테스트 환경

| 항목 | 값 |
|------|----|
| 서버 | Spring Boot (포트 8888) |
| DB | PostgreSQL 16 (HikariCP max 50, min-idle 50) |
| Cache | Redis 7 |
| MQ | Kafka 3.7 (파티션 3, 컨슈머 concurrency 3) |
| k6 | Docker (grafana/k6:latest) |

---

## 시나리오별 결과

### 1. Race Test - 동시 주문 경쟁 (재고 500, VU 1000)

1000명이 동시에 재고 500개짜리 상품에 주문.

| 지표 | 결과 | 기준 |
|------|------|------|
| 주문 성공 | 500건 | = 재고 |
| 품절 처리 | 500건 | - |
| Oversell | 0건 | 0건 |
| 서버 에러 | 0건 | 0건 |

재고 수량만큼 정확히 처리되고 Oversell 없음. Redis Lua 스크립트가 원자적으로 재고를 차감하기 때문에 1000개의 동시 요청에서도 정합성이 보장된다.

---

### 2. Stress Test - 점진적 부하 증가 (재고 500, VU 0→500)

| 단계 | VU | 시간 |
|------|----|------|
| 워밍업 | 0 → 50 | 30s |
| 정상 부하 | 50 → 150 | 60s |
| 높은 부하 | 150 → 300 | 60s |
| 임계점 | 300 → 500 | 60s |
| 복구 | 500 → 0 | 30s |

**결과:**

| 지표 | 값 |
|------|-----|
| p(95) 응답시간 | 6ms |
| p(99) 응답시간 | 22ms |
| max 응답시간 | 250ms |
| TPS | 53/s |
| 5xx 에러 | 0건 |
| TCP 연결 실패율 | 10.81% |

**해석:**

- p(95) 6ms — 2s 기준 대비 압도적으로 여유 있음
- 500 VU 극한 부하에서도 max 250ms, 서버는 안정적
- TCP 연결 실패 10.81%는 `dial: i/o timeout` — 서버 에러(5xx)가 아닌 OS 연결 큐 포화

---

### 3. Spike Test - 급격한 트래픽 폭증 (VU 20→500)

| 단계 | VU | 시간 |
|------|----|------|
| 평상시 | 0 → 20 | 20s |
| 급격한 폭증 | 20 → 500 | 5s |
| 폭증 유지 | 500 | 30s |
| 급격한 축소 | 500 → 20 | 5s |
| 복구 확인 | 20 → 0 | 20s |

**결과:**

| 지표 | 값 |
|------|-----|
| p(95) 응답시간 | 7ms |
| p(99) 응답시간 | 15ms |
| max 응답시간 | 97ms |
| TPS | 75/s |
| 5xx 에러 | 0건 |
| TCP 연결 실패율 | 11.69% |

**해석:**

- 5초 만에 20 → 500 VU로 폭증해도 p(95) 7ms — 응답시간 영향 없음
- 5xx가 0건으로, 서버가 수신한 요청은 모두 정상 처리
- TCP 연결 실패는 Stress Test와 동일한 OS 연결 큐 포화 현상

---

## 실패율 해석

```
http_req_failed 10~11%  =  dial: i/o timeout  (TCP 연결 실패)
                        !=  5xx               (서버 에러)
```

- 5xx가 0건인 것이 핵심. 서버가 받은 요청은 모두 정상 처리됨
- timeout은 서버가 받기 전에 OS 레벨 연결 큐가 포화되어 실패한 것
- 로컬 Docker 환경(단일 머신)에서 500 VU 극한 부하 시 발생하는 정상 범위

---

## 실행 방법

```bash
# 데이터 초기화 (재고 500, 사용자 1000명)
bash k6/init/setup.sh

# Race Test
docker compose run --rm -e PRODUCT_ID=1 k6 run /k6/scenarios/race.js

# Stress Test
docker compose run --rm -e PRODUCT_ID=1 k6 run /k6/scenarios/stress.js

# Spike Test
docker compose run --rm -e PRODUCT_ID=1 k6 run /k6/scenarios/spike.js

# Grafana 연동 시 --out 옵션 추가
docker compose run --rm -e PRODUCT_ID=1 k6 run --out experimental-prometheus-rw /k6/scenarios/stress.js
```

---

## HikariCP 설정

고부하 환경에서 500 VU 동시 요청 시 커넥션 풀 고갈을 방지하기 위해 아래 설정을 적용했다.

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 50
      minimum-idle: 50        # max와 동일하게 설정 — 항상 50개 유지로 burst 대비
      connection-timeout: 30000
      idle-timeout: 30000
      keepalive-time: 30000
      max-lifetime: 600000
```

- `minimum-idle: 50`: 앱 시작 시부터 50개 커넥션을 유지해 동시 요청 폭증 시 즉시 제공
- `connection-timeout: 30000`: 2초에서 30초로 완화 — 커넥션 대기 시간 확보
