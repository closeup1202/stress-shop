#!/bin/bash
# 부하 테스트 전 데이터 초기화 스크립트
# Docker Compose가 실행 중이고 Spring Boot 앱이 기동된 상태에서 실행하세요

set -e

# ─── 설정 (환경변수로 오버라이드 가능) ──────────────────
USER_COUNT=${USER_COUNT:-1000}
STOCK=${STOCK:-500}
PRICE=1000
WALLET_BALANCE=500000

PG_CONTAINER=stress-postgres
REDIS_CONTAINER=stress-redis
PG_DB=stressshop
PG_USER=postgres
# ─────────────────────────────────────────────────────

echo "=== 기존 데이터 초기화 ==="
docker exec -e PGPASSWORD=postgres "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "
DELETE FROM outbox_events;
DELETE FROM orders;
DELETE FROM wallets;
DELETE FROM stock_rollback_history;
DELETE FROM users;
DELETE FROM products;
"

echo "=== 상품 생성 (재고: $STOCK, 가격: $PRICE) ==="
docker exec -e PGPASSWORD=postgres "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "
INSERT INTO products (name, price, stock, created_at, modified_at)
VALUES ('한정판 상품', $PRICE, $STOCK, NOW(), NOW());
"

echo "=== 사용자 $USER_COUNT 명 + 지갑 생성 ==="
docker exec -e PGPASSWORD=postgres "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "
DO \$\$
DECLARE
    uid BIGINT;
BEGIN
    FOR i IN 1..$USER_COUNT LOOP
        INSERT INTO users (name, email, password, created_at, modified_at)
        VALUES (
            'user' || i,
            'user' || i || '@test.com',
            '1234',
            NOW(),
            NOW()
        )
        RETURNING id INTO uid;

        INSERT INTO wallets (user_id, balance, created_at, modified_at)
        VALUES (uid, $WALLET_BALANCE, NOW(), NOW());
    END LOOP;
END \$\$;
"

echo "=== Redis 재고 적재 ==="
PRODUCT_ID=$(docker exec -e PGPASSWORD=postgres "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -t -c "SELECT id FROM products LIMIT 1;" | tr -d ' \r\n')
docker exec "$REDIS_CONTAINER" redis-cli SET "product:stock:$PRODUCT_ID" "$STOCK"

echo ""
echo "완료"
echo "  PRODUCT_ID  : $PRODUCT_ID"
echo "  STOCK       : $STOCK"
echo "  USER_COUNT  : $USER_COUNT"
echo "  Redis key   : product:stock:$PRODUCT_ID = $STOCK"
echo ""
echo "k6 실행 명령어:"
echo "  docker compose run --rm -e PRODUCT_ID=$PRODUCT_ID k6 run /k6/scenarios/race.js"
echo "  docker compose run --rm -e PRODUCT_ID=$PRODUCT_ID k6 run /k6/scenarios/stress.js"
echo "  docker compose run --rm -e PRODUCT_ID=$PRODUCT_ID k6 run /k6/scenarios/spike.js"
echo ""
echo "k6 그라파나 연동시:"
echo "  docker compose run --rm -e PRODUCT_ID=$PRODUCT_ID k6 run --out experimental-prometheus-rw /k6/scenarios/race.js"
echo "  docker compose run --rm -e PRODUCT_ID=$PRODUCT_ID k6 run --out experimental-prometheus-rw /k6/scenarios/stress.js"
echo "  docker compose run --rm -e PRODUCT_ID=$PRODUCT_ID k6 run --out experimental-prometheus-rw /k6/scenarios/spike.js"
echo ""