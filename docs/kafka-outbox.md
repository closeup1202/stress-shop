# Kafka Outbox Pattern

## 개요

step4는 step3의 핵심 결함인 **Redis 차감 ↔ Kafka 발행 사이의 원자성 Gap**을 해결한다.

step3에서는 Kafka 발행이 Service 레이어에서 직접 일어났다. HTTP 스레드가 Redis를 차감하고 Kafka에 발행하는 두 작업은 원자적이지 않아, 발행 실패 시 catch 블록의 Redis 롤백마저 실패하면 재고가 영구 손실됐다.

step4는 Kafka 발행을 **DB 트랜잭션 내 Outbox 테이블 INSERT**로 대체한다. DB 트랜잭션이 보장하는 원자성으로 Order와 Outbox가 함께 저장되거나 함께 롤백된다. Kafka 발행은 별도의 Poller가 담당한다.

## step3 vs step4 구조 비교

```
[step3]
POST /orders
 │
 ├─ Redis 차감
 └─ Kafka 발행 ──────────────────── HTTP 스레드가 직접 발행
      │
      └─ 실패 → catch → Redis 롤백 (롤백 자체도 실패 가능)

[step4]
POST /orders
 │
 ├─ Redis 차감
 └─ @Transactional
     ├─ Order(PENDING) 저장  ─┐
     └─ OutboxEvent 저장    ──┘ 같은 트랜잭션 (원자적)
 │
 200 즉시 응답

OutboxPoller (1초마다)
 └─ Kafka 발행 → OutboxEvent → PUBLISHED
```

## 전체 흐름

```
POST /orders
 │
 ▼
[1] Redis 선차감 (Lua 스크립트)
 │
 ├── 실패 → 즉시 "품절" 반환
 │
 └── 성공
      │
      ▼
     [2] @Transactional
          ├─ Order(PENDING) INSERT
          └─ OutboxEvent(PENDING) INSERT  ← 같은 트랜잭션
      │
      200 즉시 응답
      │
      ▼ (OutboxPoller, 1초 주기)
     [3] PENDING OutboxEvent 조회 (FOR UPDATE SKIP LOCKED)
      │
      ▼
     [4] Kafka "order-create" 발행 (userId 파티션 키)
      │
      └─ 성공 → OutboxEvent → PUBLISHED
      │
      ▼ (Kafka Consumer)
     [5] Order 조회 by eventId
          멱등성 체크 (status != PENDING이면 skip)
      │
      ├─ Wallet 차감
      ├─ DB 재고 차감
      └─ order.complete() → Order(COMPLETED)
      │
      ├─ 실패 (3회 재시도) → DLQ
      │    ├─ order.fail()  → Order(FAILED)
      │    ├─ stock-rollback 발행
      │    └─ orderMetrics.incrementDlq()
      │
      ▼ (StockRollbackConsumer)
     [6] Redis 재고 복구
```

## 핵심 컴포넌트

### OrderCommandService

```java
@Service
@RequiredArgsConstructor
@Transactional
public class OrderCommandService {

    public void createOrder(Long userId, Long productId, int quantity) {

        // Redis 선차감 (트랜잭션 외부 효과)
        if (!stockCacheRepository.decrease(productId, quantity)) {
            throw new IllegalArgumentException("품절");
        }

        try {
            String eventId = UUID.randomUUID().toString();

            // Order + Outbox를 같은 트랜잭션에 저장 (핵심)
            Order order = Order.create(eventId, user, product, quantity, totalPrice);
            orderRepository.save(order);

            String payload = objectMapper.writeValueAsString(event);
            outboxRepository.save(OutboxEvent.create(eventId, "order-create", payload));

        } catch (Exception e) {
            stockCacheRepository.increase(productId, quantity);  // Redis 복구
            throw e;
        }
    }
}
```

Order와 Outbox가 같은 `@Transactional` 범위 안에 있으므로:
- DB 장애 시 둘 다 롤백 → Redis만 복구하면 됨
- 정상 시 둘 다 커밋 → Poller가 반드시 Kafka에 발행할 수 있음

### OutboxEvent

```java
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    private String id;           // eventId (Order와 동일한 UUID)

    private String topic;        // "order-create"

    @Column(columnDefinition = "TEXT")
    private String payload;      // JSON 직렬화된 이벤트

    @Enumerated(EnumType.STRING)
    private OutboxStatus status; // PENDING → PUBLISHED

    private LocalDateTime createdAt;
    private LocalDateTime publishedAt;

    public void markPublished() {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = LocalDateTime.now();
    }
}
```

### OutboxPoller

```java
@Component
@Slf4j
public class OutboxPoller implements EventPoller {

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void poll() {
        List<OutboxEvent> events = outboxRepository.findPendingWithLock(
                PageRequest.of(0, 100)
        );

        for (OutboxEvent event : events) {
            try {
                OrderCreatedEvent orderEvent = objectMapper.readValue(event.getPayload(), OrderCreatedEvent.class);
                kafkaTemplate.send(event.getTopic(), orderEvent.getUserId().toString(), orderEvent).get();
                event.markPublished();
            } catch (Exception e) {
                log.error("Outbox 발행 실패 - eventId: {}", event.getId(), e);
                // status 변경 없음 → 다음 사이클에서 재시도
            }
        }
    }
}
```

**`@Transactional` + `@Scheduled`**: `@Scheduled`는 Spring 컨텍스트의 프록시 빈을 통해 호출되므로 `@Transactional`이 정상 동작한다. 트랜잭션 범위 내에서 Pessimistic Lock을 유지하여 다른 인스턴스가 같은 레코드를 처리하지 못하게 한다.

### OutboxEventRepository (FOR UPDATE SKIP LOCKED)

```java
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "0"))
    @Query("SELECT o FROM OutboxEvent o WHERE o.status = 'PENDING' ORDER BY o.createdAt ASC")
    List<OutboxEvent> findPendingWithLock(Pageable pageable);
}
```

`lock.timeout=0`은 PostgreSQL에서 `FOR UPDATE NOWAIT`로 변환된다. 다른 인스턴스가 이미 락을 획득한 레코드에 대해 즉시 실패하여 중복 발행을 방지한다.

| 설정 | 동작 | 효과 |
|------|------|------|
| `lock.timeout=0` (NOWAIT) | 락 획득 불가 시 즉시 예외 | 다른 인스턴스가 처리 중인 레코드 건너뜀 |
| `lock.timeout` 미설정 | 락 획득할 때까지 대기 | 인스턴스 직렬화 → 처리 지연 |

### OrderEventConsumer (Order 상태 업데이트)

```java
@KafkaListener(topics = "order-create", groupId = "order-group")
@Transactional
public void consume(OrderCreatedEvent event) {
    Order order = orderRepository.findByEventId(event.getEventId()).orElseThrow();

    // 멱등성 체크: 이미 처리된 주문 스킵
    if (order.getOrderStatus() != OrderStatus.PENDING) {
        return;
    }

    walletRepository.decreaseBalance(event.getUserId(), order.getTotalPrice());
    productRepository.decreaseStock(event.getProductId(), event.getQuantity());

    order.complete();               // PENDING → COMPLETED
    orderMetrics.incrementSuccess();
}

@KafkaListener(topics = "order-create.DLQ", groupId = "order-group")
@Transactional
public void handleDlq(OrderCreatedEvent event) {
    orderRepository.findByEventId(event.getEventId())
            .ifPresent(Order::fail);    // PENDING → FAILED

    rollbackPublisher.publish(StockRollbackEvent.builder()
            .eventId(event.getEventId())
            .productId(event.getProductId())
            .quantity(event.getQuantity())
            .build());

    orderMetrics.incrementDlq();
}
```

**멱등성 체크 비교:**

| step3 | step4 |
|-------|-------|
| `existsByEventId(eventId)` | `order.getOrderStatus() != PENDING` |
| Consumer가 Order를 생성 | Service가 Order를 생성, Consumer는 상태만 변경 |

### Order 상태 전이

```
Service 호출
    │
    ▼
 PENDING ──────────────── Consumer 처리 성공 ──→ COMPLETED
    │
    └─── 재시도 3회 소진 → DLQ ──→ FAILED
```

```java
public enum OrderStatus {
    PENDING,    // 주문 접수, Consumer 처리 대기
    COMPLETED,  // Consumer 처리 성공
    FAILED,     // DLQ 도달
    SHIPPED,
    CANCELED,
}
```

### OrderMetrics

```java
@Component
public class OrderMetrics {

    public OrderMetrics(MeterRegistry registry) {
        this.successCounter = Counter.builder("order_success_total")
                .description("주문 처리 성공 수")
                .register(registry);

        this.failedCounter = Counter.builder("order_failed_total")
                .description("주문 처리 실패 수")
                .register(registry);

        this.dlqCounter = Counter.builder("order_dlq_total")
                .description("DLQ 도달 주문 수")
                .register(registry);
    }
}
```

Prometheus 엔드포인트(`/actuator/prometheus`)에서 수집:

```
# HELP order_success_total 주문 처리 성공 수
order_success_total_total 30.0

# HELP order_dlq_total DLQ 도달 주문 수
order_dlq_total_total 0.0
```

Kafka consumer lag은 `management.metrics.kafka.consumer.enabled=true` 설정 시 Micrometer가 자동 수집한다:

```
kafka_consumer_fetch_manager_records_lag{...} 0.0
```

## 멱등성 전략

| 계층 | 수단 | 보장 범위 |
|------|------|----------|
| Redis 차감 | Lua 스크립트 | 원자적 재고 차감 |
| DB 저장 | DB 트랜잭션 | Order + Outbox 동시 저장 |
| Kafka 발행 | Outbox PUBLISHED 상태 | 재시작 후에도 미발행 이벤트 재발행 |
| Consumer 처리 | `status != PENDING` 체크 | 재시도 시 중복 처리 방지 |
| 재고 롤백 | `StockRollbackHistory` PK | 롤백 중복 방지 |

## 테스트 검증 구조

step4에서 검증 타이밍이 step3과 달라진다.

```java
latch.await();   // 50개 요청 완료

// 동기 검증 (즉시) - Service에서 동기적으로 저장하므로
assertThat(orderRepository.count()).isEqualTo(stock);                        // 30
assertThat(orderRepository.countByOrderStatus(PENDING)).isEqualTo(stock);   // 30
assertThat(outboxRepository.count()).isEqualTo(stock);                       // 30

// 비동기 검증 (await) - OutboxPoller + Kafka + Consumer 완료 후
await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
    assertThat(orderRepository.countByOrderStatus(COMPLETED)).isEqualTo(stock)
);

// 최종 검증
assertThat(orderRepository.countByOrderStatus(PENDING)).isEqualTo(0);
assertThat(updatedProduct.getStock()).isEqualTo(0L);
assertThat(redisStock).isEqualTo(0L);
```

| 시점 | step3 | step4 |
|------|-------|-------|
| `latch.await()` 직후 Order 수 | 0 (Consumer 미완료) | 30 (Service에서 동기 저장) |
| await 조건 | `count() == 30` | `countByOrderStatus(COMPLETED) == 30` |
| await 이유 | Order 생성 자체가 비동기 | Order는 있지만 상태 전이가 비동기 |

## step3 대비 개선사항

| 항목 | step3 | step4 |
|------|-------|-------|
| Kafka 발행 주체 | Service (HTTP 스레드) | OutboxPoller (스케줄러) |
| 발행 실패 처리 | catch → Redis 롤백 (롤백 자체 실패 가능) | Outbox 남아서 다음 사이클 재시도 |
| Order 생성 시점 | Consumer 처리 후 | HTTP 요청 수신 즉시 (PENDING) |
| 주문 상태 조회 | 불가 | `GET /orders/{id}` → status 확인 가능 |
| 서버 크래시 복구 | Redis 차감 후 크래시 → 이벤트 영구 손실 | Outbox에 남아 재시작 후 자동 복구 |
| 멱등성 체크 기준 | `existsByEventId` | `order.status != PENDING` |

## 주의사항

### Outbox 폴링 지연
`fixedDelay = 1000`으로 최대 1초 지연이 발생한다. 실시간성이 중요한 경우 `fixedDelay`를 줄이거나, DB의 `LISTEN/NOTIFY`(PostgreSQL) 기반 CDC(Change Data Capture) 방식으로 전환할 수 있다.

### Outbox 테이블 증가
PUBLISHED 상태의 레코드가 누적된다. 배치 또는 스케줄러로 주기적으로 정리해야 한다.

```java
@Scheduled(cron = "0 0 3 * * *")  // 매일 새벽 3시
@Transactional
public void cleanPublished() {
    outboxRepository.deleteByStatusAndPublishedAtBefore(
            OutboxStatus.PUBLISHED,
            LocalDateTime.now().minusDays(7)
    );
}
```

### Redis 선차감과 Outbox의 책임 분리
- Redis 선차감 실패 → 즉시 품절 반환 (Outbox 저장 없음)
- Redis 선차감 성공 + DB 실패 → catch에서 Redis 복구, Outbox 저장 안 됨 → Kafka 발행 없음
- Redis 선차감 성공 + DB 성공 → Outbox가 Kafka 발행을 보장

Redis와 DB 사이의 불일치는 여전히 존재할 수 있으나 (Redis 성공 + DB 롤백 시 catch에서 Redis 복구),
catch의 Redis 복구(`increase`) 자체가 실패하는 케이스는 `StockSyncScheduler`가 주기적 동기화로 보완한다.
