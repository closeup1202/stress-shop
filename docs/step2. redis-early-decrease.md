# Redis 선차감 패턴

## 개요

Redis 선차감은 **DB에 접근하기 전에 Redis에서 재고를 먼저 차감**하여 트래픽을 차단하는 패턴이다.

재고가 많이 몰리는 상황에서 DB까지 요청이 도달하기 전에 Redis가 1차 관문 역할을 하여 DB 부하를 줄인다.

## 흐름

```
요청
 │
 ▼
[1] Redis 선차감 (재고 체크 + 차감 원자적 처리)
 │
 ├── 실패 (재고 없음) → 즉시 예외 반환 (DB 접근 없음)
 │
 └── 성공
      │
      ▼
     [2] DB 잔액 차감 (Compare and Set)
      │
      ▼
     [3] DB 재고 차감 (정합성 보장용)
      │
      ▼
     [4] 주문 저장
      │
      ├── 실패 → Redis 재고 롤백 (increment)
      │
      └── 성공 → 완료
```

## 구현

### StockCacheRepository (인터페이스)

```java
public interface StockCacheRepository {
    boolean decrease(Long productId, int quantity);
    void increase(Long productId, int quantity);
}
```

### RedisStockCacheRepository

```java
@Component
@RequiredArgsConstructor
public class RedisStockCacheRepository implements StockCacheRepository {

    private final StringRedisTemplate redisTemplate;

    @Override
    public boolean decrease(Long productId, int quantity) {
        String key = "product:stock:" + productId;

        Long stock = redisTemplate.opsForValue().decrement(key, quantity);

        if (stock == null || stock < 0) {
            redisTemplate.opsForValue().increment(key, quantity); // 롤백
            return false;
        }

        return true;
    }

    @Override
    public void increase(Long productId, int quantity) {
        redisTemplate.opsForValue().increment("product:stock:" + productId, quantity);
    }
}
```

`DECRBY` 명령어는 Redis의 단일 스레드 특성상 원자적으로 실행된다.
차감 후 값이 음수이면 즉시 `INCRBY`로 롤백한다.

### OrderCommandService

```java
public Long createOrder(Long userId, Long productId, int quantity) {

    // 1. Redis 선차감
    boolean redisSuccess = stockCacheRepository.decrease(productId, quantity);
    if (!redisSuccess) {
        throw new IllegalArgumentException("품절");
    }

    try {
        // 2~5. DB 처리 (잔액 차감, 재고 차감, 주문 저장)
        ...

    } catch (Exception e) {
        // DB 실패 시 Redis 롤백
        stockCacheRepository.increase(productId, quantity);
        throw e;
    }
}
```

## Redis key 구조

```
product:stock:{productId}
예) product:stock:1  →  "100"
```

값은 String 타입으로 저장하며 `StringRedisTemplate`을 사용한다.

## DB 재고 차감을 유지하는 이유

Redis 선차감이 성공해도 DB 재고 차감(`decreaseStock`)은 여전히 수행한다.

- Redis는 캐시이므로 재시작 시 데이터가 사라질 수 있음
- DB가 재고의 최종 원천(source of truth)
- Redis 선차감은 트래픽 차단 역할, DB 차감은 데이터 정합성 보장 역할

## Compare and Set 패턴과 비교

| 항목 | Compare and Set (step1) | Redis 선차감 (step2) |
|------|------------------------|---------------------|
| 재고 차감 위치 | DB | Redis → DB |
| DB 쿼리 수 | UPDATE 1회 | SELECT 1회 + UPDATE 1회 |
| 트래픽 차단 위치 | DB 레벨 | Redis 레벨 (DB 앞단) |
| Redis 의존성 | 없음 | 있음 |
| 적합한 상황 | 일반적인 동시성 제어 | 대량 트래픽, 품절 빠른 감지 |

## Redis ↔ DB 정합성 복구 전략

Redis 선차감과 DB 차감 사이는 완전한 원자성이 보장되지 않아 불일치가 발생할 수 있다.

### 불일치 시나리오

| 시나리오 | 결과 |
|----------|------|
| Redis 차감 성공 → DB 처리 중 JVM crash | Redis만 차감된 채로 남음 (재고 손실) |
| catch 블록의 Redis 롤백 자체 실패 | 동일 |
| Redis 재시작 (영속화 없음) | Redis key 초기화, DB와 불일치 |

### 복구 전략: DB → Redis 주기적 동기화 (StockSyncScheduler)

30초마다 DB 재고를 Redis에 동기화하는 스케줄러로 복구한다.

```java
@Scheduled(fixedDelay = 30000)
public void syncStock() {
    List<Product> products = productRepository.findAll();

    for (Product product : products) {
        String key = "product:stock:" + product.getId();
        redisTemplate.opsForValue().setIfAbsent(key, String.valueOf(product.getStock()));
    }
}
```

**`SET` 대신 `SETNX`(`setIfAbsent`)를 사용하는 이유:**

`SET`으로 덮어쓰면 스케줄러가 DB를 읽는 시점과 Redis에 쓰는 시점 사이에 트랜잭션이 끼어들 수 있다.

```
T=1  Redis 차감 → Redis stock = 4
T=2  스케줄러: DB stock = 5 읽음  (차감 전 스냅샷)
T=3  DB 재고 차감 완료 → DB stock = 4
T=4  스케줄러: Redis stock = 5 덮어씀  ← 잘못된 값
```

`setIfAbsent`를 사용하면 **key가 없을 때만** (Redis 재시작 등으로 날아간 경우) DB 값으로 초기화하므로, 정상 운영 중인 재고를 덮어쓰지 않는다.

### 이 전략의 한계

- key는 살아있지만 값이 잘못된 경우 (`setIfAbsent`로 복구 불가)
- Redis 재시작 후 스케줄러 실행 전 30초간 선차감 불가 (key 없으므로 `DECRBY` 시 음수 처리됨)
- 완전한 해결책은 Redis 영속화(AOF) 또는 보상 이벤트 큐 도입 필요

## 주의사항

- Redis에 재고를 미리 적재(warm-up)해야 하며, 초기 동기화 시점 관리가 필요함
- `@EnableScheduling`을 `ApiApplication`에 추가해야 스케줄러가 동작함
