# Compare and Set 패턴

## 개요

Compare and Set(CAS)은 **조건 비교(Compare)와 값 세팅(Set)을 단일 SQL로 원자적으로 처리**하는 패턴이다.

읽기 → 검증 → 쓰기를 애플리케이션 레벨에서 분리하지 않고, DB가 하나의 쿼리로 처리하기 때문에 동시성 문제를 DB 레벨에서 해결한다.

## 구조

```sql
UPDATE {table}
SET {column} = {new_value}
WHERE {id} = :id
AND {condition}   -- Compare: 조건이 참일 때만
                  -- Set: 값을 세팅
```

- `affected rows = 1` → 조건 충족, 업데이트 성공
- `affected rows = 0` → 조건 불충족, 업데이트 실패

## 적용 예시

### 재고 차감 (ProductRepository)

```java
@Modifying
@Query("""
    UPDATE Product p
    SET p.stock = p.stock - :quantity
    WHERE p.id = :productId
    AND p.stock >= :quantity
""")
int decreaseStock(@Param("productId") Long productId, @Param("quantity") long quantity);
```

- **Compare**: `p.stock >= :quantity` → 재고가 충분한지 비교
- **Set**: `p.stock = p.stock - :quantity` → 재고 차감

### 잔액 차감 (WalletRepository)

```java
@Modifying
@Query("""
    UPDATE Wallet w
    SET w.balance = w.balance - :amount
    WHERE w.id = :userId
    AND w.balance >= :amount
""")
int decreaseBalance(@Param("userId") Long userId, @Param("amount") BigDecimal amount);
```

- **Compare**: `w.balance >= :amount` → 잔액이 충분한지 비교
- **Set**: `w.balance = w.balance - :amount` → 잔액 차감

### 서비스 레이어에서의 처리 (OrderCommandService)

```java
int walletUpdated = walletRepository.decreaseBalance(userId, totalPrice);
if (walletUpdated == 0) {
    throw new IllegalArgumentException("잔액 부족");
}

int updated = productRepository.decreaseStock(productId, quantity);
if (updated == 0) {
    throw new IllegalArgumentException("재고 부족");
}
```

`affected rows`가 0이면 조건 불충족으로 판단해 예외를 던진다.

## 낙관적 락(@Version)과 비교

| 항목 | 낙관적 락 (@Version)              | Compare and Set |
|------|-------------------------------|-----------------|
| 충돌 감지 방식 | version 값 비교                  | 비즈니스 조건 비교 |
| 실패 시 처리 | OptimisticLockException → 재시도 | affected rows = 0 → 예외 |
| SELECT 쿼리 | 필요 (엔티티 조회)                   | 불필요 |
| 재시도 로직 | 필요할 수 있음 (거의 필요)              | 불필요 |
| 용도 | 일반적인 충돌 방지                    | 특정 조건(재고, 잔액 등) 기반 업데이트 |

## 장점

- SELECT 없이 UPDATE 한 번으로 처리 → 쿼리 수 감소
- DB 레벨에서 원자성 보장 → 동시성 문제 없음
- 재시도 로직 불필요

## 주의사항

- `@Modifying` 어노테이션 필요 (JPA bulk update)
- 영속성 컨텍스트와 DB 상태가 불일치할 수 있으므로, 필요 시 `@Modifying(clearAutomatically = true)` 사용
- 엔티티의 도메인 메서드(`withdraw`, `decreaseStock`)를 우회하므로, 해당 로직이 필요 없는 경우에만 적용
