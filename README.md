# Stress Shop

한정판 상품 판매 시나리오를 통해 대규모 트래픽에서 발생하는 동시성 문제를 단계별로 해결하는 실습 프로젝트

## 시나리오

- 재고 30개의 한정판 상품을 50명이 동시에 주문
- 정확히 30건의 주문만 성공해야 하며, 재고 초과 판매(Oversell)가 없어야 한다

    (k6 부하 테스트의 경우 재고 500개 / 사용자 1000명)

## 기술 스택

| 분류 | 기술                  |
|------|---------------------|
| Language | Java 17             |
| Framework | Spring Boot 3       |
| DB | PostgreSQL          |
| Cache | Redis               |
| Message Queue | Apache Kafka        |
| Monitoring | Prometheus, Grafana |
| Container | Docker Compose      |

---

## 단계별 구현 전략

| Step | 브랜치 | 핵심 전략 | 처리 방식 |
|------|--------|----------|----------|
| Step 0 | `step0-optimistic-lock` | JPA `@Version` 낙관적 락 | DB Row Lock |
| Step 1 | `step1-cas` | DB CAS (UPDATE WHERE 조건) | DB Atomic Write |
| Step 2 | `step2-redis` | Redis 선차감 + DB CAS | Redis → DB |
| Step 3 | `step3-mq-async` | Redis 선차감 + Kafka 비동기 | Redis → MQ → Consumer |
| Step 4 | `step4-outbox` | Outbox Pattern + Order 상태 관리 | Redis → DB(Outbox) → MQ → Consumer |
| Step 5 | `step5-e2e-load-test` | k6 부하 테스트 (정합성 + 성능 검증) | Smoke / Race / Stress 시나리오 |

---

## Step 0 - 낙관적 락 (Optimistic Lock)

JPA의 `@Version` 필드를 이용해 동시 수정 충돌을 감지한다.

```java
@Version
private Long version;
```

트랜잭션이 커밋될 때 version 값을 비교해 충돌이 감지되면 `OptimisticLockException`을 던진다.

**문제점**: 충돌 시 재시도 로직이 필요하고, 경합이 심할수록 재시도가 폭증해 DB 부하가 증가한다.

---

## Step 1 - DB Compare and Set

SELECT 없이 UPDATE 한 번으로 조건 비교와 차감을 원자적으로 처리한다.

```sql
UPDATE product
SET stock = stock - :quantity
WHERE id = :productId
AND stock >= :quantity
```

`affected rows = 0`이면 재고 부족으로 판단한다. 재시도 없이 단일 쿼리로 동시성을 해결한다.

**문제점**: 모든 요청이 DB까지 도달하므로 대규모 트래픽 시 DB가 병목이 된다.

> 상세 내용: [docs/compare-and-set.md](docs/step1.%20compare-and-set.md)

---

## Step 2 - Redis 선차감

DB 앞에 Redis를 배치해 트래픽을 1차로 차단한다.

```
요청 → Redis 차감 (DECRBY) → 성공 시 DB 처리
                            → 실패 시 즉시 품절 반환 (DB 접근 없음)
```

`DECRBY`로 차감 후 음수이면 `INCRBY`로 즉시 보상한다. DB 부하를 크게 줄일 수 있다.

**문제점**: HTTP 스레드가 DB 처리(잔액 차감, 재고 차감, 주문 저장)가 완료될 때까지 대기한다.

> 상세 내용: [docs/redis-early-decrease.md](docs/step2.%20redis-early-decrease.md)

---

## Step 3 - Kafka 비동기 처리

DB 처리를 Kafka Consumer에게 위임해 HTTP 스레드를 즉시 해제한다.

```
요청 → Redis 차감 → Kafka 발행 → 200 즉시 응답
                                      ↓ (비동기)
                               Kafka Consumer → Wallet + Stock + Order 저장
```

- **userId 파티션 키**: 동일 사용자의 요청이 같은 파티션으로 라우팅되어 순서 보장
- **DLQ 패턴**: 재시도 3회 소진 후 DLQ 토픽으로 이동, 그 시점에만 Redis 재고 롤백
- **concurrency=3**: 파티션 3개와 1:1로 Consumer 스레드 배정

**문제점**: Kafka 발행이 실패하면 catch 블록의 Redis 롤백마저 실패할 수 있다. 발행과 롤백 사이의 원자성이 보장되지 않는다.

> 상세 내용: [docs/kafka-mq-async.md](docs/step3.%20kafka-mq-async.md)

---

## Step 4 - Outbox Pattern

Kafka 발행을 DB 트랜잭션 내 Outbox 테이블 INSERT로 대체해 원자성을 보장한다.

```
요청 → Redis 차감 → @Transactional ─┬─ Order(PENDING) 저장
                                    └─ OutboxEvent(PENDING) 저장
                   → 200 즉시 응답

OutboxPoller (1초마다) → Kafka 발행 → OutboxEvent → PUBLISHED

Kafka Consumer → Wallet + Stock + order.complete() → Order(COMPLETED)
              → 실패(DLQ) → order.fail() → Order(FAILED) + Redis 롤백
```

- **Outbox 원자성**: Order와 OutboxEvent가 같은 트랜잭션으로 저장 — 둘 다 저장되거나 둘 다 롤백
- **서버 크래시 복구**: 재시작 후 Poller가 PENDING Outbox를 자동으로 재발행
- **Order 상태 추적**: PENDING → COMPLETED / FAILED 상태로 주문 처리 결과 조회 가능
- **Prometheus Metrics**: `order_success_total`, `order_failed_total`, `order_dlq_total`

> 상세 내용: [docs/kafka-outbox.md](docs/step4.%20kafka-outbox.md)

---

## Step 5 - k6 부하 테스트

코드 변경 없이 Step 4 시스템을 k6로 검증한다. 네 가지 시나리오로 정합성과 성능을 확인한다.

| 시나리오 | 목적 | 파일 |
|---------|------|------|
| **Smoke** | 배포 직후 최소 동작 확인 (VU 1, 30s) | `k6/scenarios/smoke.js` |
| **Race** | 동시 주문 경쟁 — Oversell 없이 재고만큼만 처리되는지 검증 | `k6/scenarios/race.js` |
| **Stress** | 점진적 부하 증가 (0→500 VU) — TPS·응답시간 측정 | `k6/scenarios/stress.js` |
| **Spike** | 급격한 트래픽 폭증 — 스파이크 시 안정성과 복구 확인 | `k6/scenarios/spike.js` |

**Race Test 결과** (재고 500, VU 1000 동시 주문)

| 지표 | 결과 |
|------|------|
| 주문 성공 | 500건 (= 재고, Oversell 0건) |
| 품절 처리 | 500건 |
| 서버 에러 | 0건 |

**Stress Test 결과** (재고 500, VU 0→500)

| 지표 | 결과 |
|------|------|
| p(95) 응답시간 | 6ms |
| p(99) 응답시간 | 22ms |
| TPS | 53/s |
| 5xx 에러 | 0건 |
| TCP 연결 실패 | 10.81% |

**Spike Test 결과** (VU 20→500 급격한 폭증)

| 지표 | 결과 |
|------|------|
| p(95) 응답시간 | 7ms |
| p(99) 응답시간 | 15ms |
| 5xx 에러 | 0건 |
| TCP 연결 실패 | 11.69% |

TCP 연결 실패는 `dial: i/o timeout`으로 서버 에러가 아닌 OS 연결 큐 포화 현상이며, 서버가 수신한 요청은 100% 정상 처리됐다.

> 상세 내용: [docs/k6-load-test.md](docs/k6-load-test.md)

---

## 단계별 비교표

### 처리 구조

| 항목 | Step 0 | Step 1 | Step 2 | Step 3 | Step 4 |
|------|--------|--------|--------|--------|--------|
| DB 부하 차단 | 없음 | 없음 | Redis | Redis | Redis |
| HTTP 스레드 응답 시점 | DB 완료 후 | DB 완료 후 | DB 완료 후 | Kafka 발행 후 | Outbox 저장 후 |
| Kafka 발행 주체 | - | - | - | Service (HTTP 스레드) | OutboxPoller (스케줄러) |
| Kafka 발행 원자성 | - | - | - | 없음 (catch 롤백 의존) | DB 트랜잭션 보장 |
| 주문 상태 추적 | 없음 | 없음 | 없음 | 없음 | PENDING / COMPLETED / FAILED |
| 서버 크래시 복구 | - | - | - | 이벤트 유실 가능 | Outbox 재발행 |

### 동시성 제어

| 항목 | Step 0 | Step 1 | Step 2 | Step 3 | Step 4 |
|------|--------|--------|--------|--------|--------|
| 재고 차감 위치 | DB | DB | Redis → DB | Redis → DB (Consumer) | Redis → DB (Consumer) |
| 동시성 수단 | `@Version` (낙관적 락) | UPDATE WHERE (CAS) | DECRBY + INCRBY 보상 | Lua 스크립트 | Lua 스크립트 |
| 충돌 시 처리 | OptimisticLockException → 재시도 | affected rows = 0 | stock < 0 → INCRBY 복구 | result < 0 (Lua) | result < 0 (Lua) |
| 재시도 필요 여부 | 필요 | 불필요 | 불필요 | 불필요 | 불필요 |

### 장애 대응

| 항목 | Step 0 | Step 1 | Step 2 | Step 3 | Step 4 |
|------|--------|--------|--------|--------|--------|
| DB 장애 | 전체 실패 | 전체 실패 | 전체 실패 | Consumer 재시도 | Consumer 재시도 |
| Redis 장애 | - | - | 전체 실패 | 전체 실패 | 전체 실패 |
| Kafka 장애 | - | - | - | 발행 실패 → Redis 롤백 | Outbox에 보관 → 복구 후 재발행 |
| Consumer 실패 | - | - | - | DLQ → Redis 롤백 | DLQ → Order FAILED + Redis 롤백 |
| 서버 재시작 | - | - | - | 미발행 이벤트 유실 | Outbox에 남아 재시작 후 자동 복구 |

### 멱등성

| 계층 | Step 3 | Step 4 |
|------|--------|--------|
| Consumer 중복 처리 방지 | `existsByEventId` | `order.status != PENDING` |
| 재고 롤백 중복 방지 | `StockRollbackHistory` PK | `StockRollbackHistory` PK |
| Outbox 중복 발행 방지 | - | `FOR UPDATE NOWAIT` |

---

## 인프라 구성

```
PostgreSQL :5432    Redis :6379
Kafka :9092 (외부)  Kafka :29092 (내부)
Kafka UI :8080
Prometheus :9090    Grafana :3000
```

```bash
docker compose up -d
```

## 모니터링

| 엔드포인트 | 내용 |
|-----------|------|
| `GET /actuator/prometheus` | Prometheus 메트릭 수집 |
| `GET /actuator/health` | 헬스 체크 |
| `http://localhost:8080` | Kafka UI |
| `http://localhost:9090` | Prometheus |
| `http://localhost:3000` | Grafana (admin/admin) |

### 주요 메트릭

| 메트릭 | 설명 |
|--------|------|
| `order_success_total` | 주문 처리 성공 수 |
| `order_failed_total` | 주문 처리 실패 수 |
| `order_dlq_total` | DLQ 도달 주문 수 |
| `kafka_consumer_fetch_manager_records_lag` | Kafka Consumer Lag |

## 문서

| 문서 | 내용 |
|------|------|
| [compare-and-set.md](docs/step1.%20compare-and-set.md) | Step 1 - DB CAS 패턴 |
| [redis-early-decrease.md](docs/step2.%20redis-early-decrease.md) | Step 2 - Redis 선차감 패턴 |
| [kafka-mq-async.md](docs/step3.%20kafka-mq-async.md) | Step 3 - Kafka 비동기 처리 |
| [kafka-outbox.md](docs/step4.%20kafka-outbox.md) | Step 4 - Outbox Pattern |
| [k6-load-test.md](docs/k6-load-test.md) | Step 5 - k6 부하 테스트 |