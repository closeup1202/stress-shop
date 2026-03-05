# Kafka MQ 비동기 처리 패턴

## 개요

step3은 주문 요청을 **Kafka 메시지로 비동기 처리**하는 패턴이다.

HTTP 요청 스레드는 Redis 재고 차감 후 Kafka에 이벤트를 발행하고 즉시 응답한다.
실제 DB 처리(잔액 차감, 재고 차감, 주문 저장)는 Kafka Consumer가 비동기로 수행한다.

## 전체 흐름

```
클라이언트 요청
 │
 ▼
[1] Redis 선차감 (Lua 스크립트, 원자적)
 │
 ├── 실패 (재고 없음) → 즉시 "품절" 반환
 │
 └── 성공
      │
      ▼
     [2] Kafka "order-create" 이벤트 발행 (userId 파티션 키)
      │
      ├── 실패 → Redis 재고 롤백 (rollback)
      │
      └── 성공 → HTTP 200 즉시 응답 (비동기)
           │
           ▼ (Kafka Consumer 처리)
          [3] 멱등성 체크 (existsByEventId)
           │
           ▼
          [4] Wallet 잔액 차감 (Compare and Set)
           │
          [5] DB 재고 차감
           │
          [6] Order 저장
           │
           ├── 실패 (재시도 3회) → DLQ 전송
           │                         │
           │                         ▼
           │                    [7] Redis 재고 롤백
           │
           └── 성공 → 완료
```

## 핵심 컴포넌트

### OrderCommandService (Producer 측)

```java
public void createOrder(Long userId, Long productId, int quantity) {
    // 1. Redis 선차감 (Lua 스크립트)
    if (!stockCacheRepository.decrease(productId, quantity)) {
        throw new IllegalArgumentException("품절");
    }

    String eventId = UUID.randomUUID().toString();

    try {
        eventPublisher.publish(OrderCreatedEvent.builder()
                .eventId(eventId)
                .userId(userId)
                .productId(productId)
                .quantity(quantity)
                .build());
    } catch (Exception e) {
        // 2. Kafka 발행 실패 시 Redis 롤백
        stockCacheRepository.increase(productId, quantity);
        throw e;
    }
}
```

### RedisStockCacheRepository (Lua 스크립트)

```java
private static final RedisScript<Long> DECREASE_SCRIPT = RedisScript.of("""
    local stock = tonumber(redis.call('GET', KEYS[1]))
    if stock == nil then
        return -2
    end
    if stock < tonumber(ARGV[1]) then
        return -1
    end
    return redis.call('DECRBY', KEYS[1], ARGV[1])
    """, Long.class);

public boolean decrease(Long productId, int quantity) {
    String key = "product:stock:" + productId;
    Long result = redisTemplate.execute(DECREASE_SCRIPT, List.of(key), String.valueOf(quantity));
    return result != null && result >= 0;
}
```

**Lua 스크립트를 사용하는 이유:**
DECR 후 음수이면 INCR로 보상하는 기존 방식은 두 명령 사이에 다른 명령이 끼어들 수 있다.
Lua 스크립트는 Redis 서버에서 원자적으로 실행되므로 TOCTTOU 경쟁 조건이 없다.

| 방식 | 문제 |
|------|------|
| DECRBY → 음수 확인 → INCRBY | DECRBY와 INCRBY 사이에 다른 클라이언트의 읽기가 끼어들 수 있음 |
| Lua 스크립트 | GET + 조건 + DECRBY가 단일 원자 연산으로 실행됨 |

### OrderEventPublisher (파티션 키)

```java
public void publish(OrderCreatedEvent event) {
    try {
        kafkaTemplate.send("order-create", event.getUserId().toString(), event).get();
    } catch (Exception e) {
        throw new RuntimeException("Kafka 발행 실패", e);
    }
}
```

`userId`를 파티션 키로 사용하면 **같은 사용자의 주문은 항상 같은 파티션으로 라우팅**된다.
파티션 내에서 순서가 보장되므로 동일 사용자의 중복 요청이 순서대로 처리된다.
`.get()`으로 동기 확인하여 발행 실패를 즉시 감지한다.

### OrderEventConsumer

```java
@KafkaListener(topics = "order-create", groupId = "order-group")
@Transactional
public void consume(OrderCreatedEvent event) {
    // 멱등성 체크 - 재시도 시 중복 처리 방지
    if (orderRepository.existsByEventId(event.getEventId())) {
        return;
    }

    // 잔액 차감 (Compare and Set)
    int walletUpdated = walletRepository.decreaseBalance(event.getUserId(), totalPrice);
    if (walletUpdated == 0) {
        throw new RuntimeException("잔액 부족");
    }

    // DB 재고 차감
    int updated = productRepository.decreaseStock(event.getProductId(), event.getQuantity());
    if (updated == 0) {
        throw new RuntimeException("DB 재고 부족");
    }

    orderRepository.save(Order.create(...));
}

// 재시도 3회 소진 후 DLQ 도달 시 재고 롤백
@KafkaListener(topics = "order-create.DLQ", groupId = "order-group")
public void handleDlq(OrderCreatedEvent event) {
    rollbackPublisher.publish(StockRollbackEvent.builder()
            .eventId(event.getEventId())
            .productId(event.getProductId())
            .quantity(event.getQuantity())
            .build());
}
```

**DLQ 전용 롤백 전략:**
재시도 중에 롤백하면 나중 재시도가 성공해도 재고가 복구된 상태가 된다 (oversell).
오직 DLQ 도달 시에만 롤백하여 이 문제를 방지한다.

### KafkaConfig (DLQ + 재시도)

```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
    FixedBackOff backOff = new FixedBackOff(1000L, 3);  // 1초 간격, 3회 재시도

    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
            (record, ex) -> new TopicPartition(record.topic() + ".DLQ", record.partition())
    );

    return new DefaultErrorHandler(recoverer, backOff);
}
```

재시도 3회 소진 후 `order-create.DLQ` 토픽으로 자동 전송된다.
DLQ 파티션은 원본 토픽과 동일한 파티션 번호를 유지하여 파티션 키 기반 순서를 보존한다.

### StockRollbackConsumer

```java
@KafkaListener(topics = "stock-rollback", groupId = "stock-rollback-group")
@Transactional
public void rollback(StockRollbackEvent event) {
    // 멱등성 체크
    if (historyRepository.existsById(event.getEventId())) {
        return;
    }
    stockCacheRepository.increase(event.getProductId(), event.getQuantity());
    historyRepository.save(new StockRollbackHistory(event.getEventId()));
}
```

`stock-rollback-group`을 별도로 분리하여 `order-group`의 rebalance 영향을 받지 않는다.
`StockRollbackHistory`에 `eventId`를 PK로 저장하여 멱등성을 보장한다.

## 멱등성 전략

| 계층 | 멱등성 수단 |
|------|------------|
| Redis 재고 차감 | Lua 스크립트로 원자적 처리 |
| Order Consumer | `existsByEventId` + Order 테이블 eventId unique 제약 |
| StockRollback Consumer | `existsById` + `StockRollbackHistory` PK |

## 병렬 처리 구성

```yaml
kafka:
  listener:
    ack-mode: record
    concurrency: 3
```

- `concurrency: 3` - 컨슈머 인스턴스 3개, `order-create` 토픽 파티션 3개와 1:1 매핑
- `ack-mode: record` - 각 메시지 처리 후 즉시 오프셋 커밋 (배치 커밋 대비 처리 실패 시 재전송 범위 최소화)
- `KAFKA_NUM_PARTITIONS=3` - docker-compose 기본 파티션 수 3개로 설정

```
order-create 토픽
├── partition 0 ──→ consumer thread 0  (userId % 3 == 0)
├── partition 1 ──→ consumer thread 1  (userId % 3 == 1)
└── partition 2 ──→ consumer thread 2  (userId % 3 == 2)
```

## 인프라 구성 (docker-compose)

```yaml
kafka:
  image: apache/kafka:3.7.0
  environment:
    - KAFKA_LISTENERS=EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
    - KAFKA_ADVERTISED_LISTENERS=EXTERNAL://localhost:9092,INTERNAL://stress-kafka:29092
    - KAFKA_NUM_PARTITIONS=3
    - KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0
```

| 리스너 | 포트 | 용도 |
|--------|------|------|
| EXTERNAL | 9092 | 호스트 → Kafka (Spring 앱, 테스트) |
| INTERNAL | 29092 | 컨테이너 간 통신 (kafka-ui 등) |
| CONTROLLER | 9093 | KRaft 컨트롤러 선출 |

`KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0` - 컨슈머 그룹 첫 조인 시 rebalance 대기 시간 제거.
기본값 3000ms이면 컨테이너 기동 후 3초간 메시지가 처리되지 않아 테스트가 타임아웃될 수 있다.

## step2 대비 변경사항

| 항목 | step2 (Redis 선차감) | step3 (Kafka 비동기) |
|------|---------------------|---------------------|
| DB 처리 시점 | 요청 스레드에서 동기 | Kafka Consumer에서 비동기 |
| 응답 시점 | DB 처리 완료 후 | Kafka 발행 성공 후 즉시 |
| 재고 롤백 | catch 블록에서 즉시 | DLQ 도달 시에만 |
| 장애 격리 | DB 장애 시 요청 실패 | DB 장애 시 재시도 후 DLQ |
| Redis 차감 방식 | DECRBY + 보상 INCRBY | Lua 스크립트 (원자적) |
| 처리 순서 보장 | 없음 | userId 파티션 키로 동일 사용자 순서 보장 |

## 주의사항

### @Transactional + @KafkaListener
`@Transactional`을 `@KafkaListener` 메서드에 직접 붙이면 Spring AOP 프록시를 통해 동작하므로 기능상 문제는 없다.
단, 명확한 관심사 분리를 위해 비즈니스 로직을 별도 `@Service`로 분리하는 것을 권장한다.

### Redis key 초기화
테스트 또는 서비스 기동 전 Redis에 재고 key를 반드시 적재(warm-up)해야 한다.
key가 없으면 Lua 스크립트가 `-2`를 반환하여 품절 처리된다.

```java
// 상품 등록 또는 서버 기동 시
redisTemplate.opsForValue().set("product:stock:" + productId, String.valueOf(stock));
```

### concurrency vs 파티션 수
`concurrency`가 파티션 수보다 크면 초과 스레드는 유휴 상태가 된다.
`stock-rollback` 토픽은 파티션 1개이므로 `concurrency: 3` 설정 시 스레드 2개가 놀게 된다.
필요하면 `@KafkaListener(concurrency = "1")`로 개별 오버라이드할 수 있다.
