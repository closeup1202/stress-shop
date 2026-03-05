# Step 5: k6 부하 테스트

## 개요

Redis 선차감 + Kafka Outbox 패턴으로 구성된 주문 시스템의 정합성과 성능을 k6로 검증한다.

- **재고 정합성**: Oversell(초과 판매) 없이 정확히 재고만큼만 주문이 처리되는지
- **응답시간**: 고부하에서도 p95 < 2s를 유지하는지
- **처리량(TPS)**: VU 증가에 따른 처리량 변화

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
| DB | PostgreSQL 16 (HikariCP max 50) |
| Cache | Redis 7 |
| MQ | Kafka 3.7 (파티션 3, 컨슈머 concurrency 3) |
| k6 | Docker (grafana/k6:latest) |

---

## 시나리오별 결과

### 1. Race Test - 동시 주문 경쟁 (재고 100, VU 200)

200명이 동시에 재고 100개짜리 상품에 주문.

| 지표 | 결과 | 기준 |
|------|------|------|
| 주문 성공 | 100건 | = 재고 |
| 품절 처리 | 100건 | - |
| Oversell | 0건 | 0건 |
| 서버 에러 | 0건 | 0건 |

재고 수량만큼 정확히 처리되고 Oversell 없음. Redis Lua 스크립트가 원자적으로 재고를 차감하기 때문에 동시 요청에서도 정합성이 보장된다.

---

### 2. Stress Test - 점진적 부하 증가 (재고 10000, VU 0→500)

| 단계 | VU | 시간 |
|------|----|------|
| 워밍업 | 0 → 50 | 30s |
| 정상 부하 | 50 → 150 | 60s |
| 높은 부하 | 150 → 300 | 60s |
| 임계점 | 300 → 500 | 60s |
| 복구 | 500 → 0 | 30s |

**결과:**

| 지표 | 150 VU | 500 VU |
|------|--------|--------|
| p(95) 응답시간 | 11ms | 279ms |
| p(99) 응답시간 | 26ms | 540ms |
| TPS | 36/s | 86/s |
| 실패율 | 4.70% | 6.04% |
| 5xx 에러 | 0건 | 0건 |
| Oversell | 0건 | 0건 |

**해석:**

- 500 VU까지 p(95) 279ms — 2s 기준 대비 여유 있음
- TPS는 VU 증가에 따라 선형적으로 증가 (36 → 86/s)
- 실패율 6.04%는 모두 `dial: i/o timeout` — 500 VU 극한 부하에서 TCP 연결 큐 초과로 발생. 5xx(서버 에러)와는 다름
- Oversell은 어떤 부하에서도 발생하지 않음

---

## 실패율 해석

```
http_req_failed 6.04%  =  dial: i/o timeout  (TCP 연결 실패)
                       !=  5xx               (서버 에러)
```

- 5xx가 0건인 것이 핵심. 서버가 받은 요청은 모두 정상 처리됨
- timeout은 서버가 받기 전에 연결 자체가 실패한 것으로, 임계점 탐색 목적의 stress test에서는 정상 범위

---

## 실행 방법

```bash
# 데이터 초기화
bash k6/init/setup.sh

# Race Test (500명이 재고 10000개에 동시 주문)
docker compose run --rm \
  -e PRODUCT_ID=1 \
  -e STOCK=10000 \
  -e USER_COUNT=500 \
  k6 run /k6/scenarios/race.js

# Stress Test (0 → 500 VU 점진적 부하)
docker compose run --rm -e PRODUCT_ID=1 k6 run /k6/scenarios/stress.js
```

자세한 실행 가이드는 [k6/README.md](../k6/README.md) 참조.

---

## HikariCP 설정

고부하 환경에서 idle 연결이 PostgreSQL에 의해 닫히는 문제를 방지하기 위해 아래 설정을 적용했다.

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 50
      minimum-idle: 10
      connection-timeout: 2000
      idle-timeout: 30000    # 30초 idle 연결 선제 해제
      keepalive-time: 30000  # 30초마다 ping으로 연결 유지
      max-lifetime: 600000   # 10분 후 연결 교체
```

- `idle-timeout`: Hikari가 먼저 idle 연결을 정리해 PostgreSQL이 끊기 전에 교체
- `keepalive-time`: 장시간 idle 연결에 ping을 보내 연결 유지
