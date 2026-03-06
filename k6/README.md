# k6 부하 테스트

k6를 Docker로 실행합니다. 별도 설치 없이 `docker compose run`으로 구동합니다.

## 사전 요구사항

- Docker Compose 실행 중 (PostgreSQL, Redis, Kafka, k6 포함)
- Spring Boot 앱 실행 중 (포트 8888)

```bash
# 인프라 기동
docker compose up -d

# Spring Boot 실행
./gradlew :stress-api:bootRun
```

---

## 테스트 데이터 초기화

k6 실행 전 반드시 데이터를 초기화해야 합니다.
`ddl-auto: create` 설정으로 앱 재시작 시 테이블이 초기화되므로 **앱 기동 후** 실행하세요.

```bash
bash k6/init/setup.sh
```

출력 예시:
```
✓ 완료
  PRODUCT_ID  : 1
  STOCK       : 500
  USER_COUNT  : 1000
  Redis key   : product:stock:1 = 500
```

> setup.sh 출력의 PRODUCT_ID가 1이 아닌 경우 아래 명령어에서 PRODUCT_ID 값을 변경하세요.

---

## 시나리오 실행

k6는 `docker compose run`으로 실행합니다.
컨테이너 내부에서 `host.docker.internal:8888`로 Spring Boot에 접근합니다.

### 1. Smoke Test - 기본 동작 확인

```bash
docker compose run --rm k6 run /k6/scenarios/smoke.js
```

| 항목 | 설정 |
|------|------|
| VU | 1 |
| 시간 | 30s |
| 목적 | 배포 후 기본 동작 확인 |
| 통과 기준 | p95 < 1s, 5xx = 0 |

---

### 2. Race Test - 동시 주문 경쟁

```bash
docker compose run --rm \
  -e PRODUCT_ID=1 \
  -e STOCK=500 \
  -e USER_COUNT=1000 \
  k6 run /k6/scenarios/race.js
```

| 항목 | 설정                  |
|------|---------------------|
| VU | 1000 (= USER_COUNT) |
| iterations | 500 (각 VU 1회)       |
| 목적 | Oversell 발생 여부 확인   |
| 통과 기준 | 성공 건수 ≤ 재고, 5xx = 0 |

**기대 결과:**
```
=== 동시 주문 경쟁 결과 ===
총 요청     : 1000건
주문 성공   : 500건  (기대: 500건)
품절 처리   : 500건
서버 에러   : 0건
Oversell    : ✅ 없음
```

---

### 3. Stress Test - 점진적 부하 증가

```bash
docker compose run --rm \
  -e PRODUCT_ID=1 \
  k6 run /k6/scenarios/stress.js
```

| 단계 | VU | 시간 | 관찰 포인트 |
|------|----|------|------------|
| 워밍업 | 0 → 20 | 30s | JVM 웜업, 첫 응답 |
| 정상 부하 | 20 → 50 | 60s | 안정적인 TPS |
| 높은 부하 | 50 → 100 | 60s | 응답시간 증가 시작점 |
| 임계점 | 100 → 150 | 60s | 에러 발생 여부, DB/Redis 병목 |
| 복구 | 150 → 0 | 30s | 부하 해제 후 정상 복귀 |

**통과 기준:** p95 < 2s, p99 < 5s, 5xx < 1%

---

## 환경 변수

docker-compose.yaml의 k6 서비스에 기본값이 설정되어 있습니다.
`-e` 옵션으로 오버라이드할 수 있습니다.

| 변수 | 기본값                                | 설명 |
|------|------------------------------------|------|
| `BASE_URL` | `http://host.docker.internal:8888` | Spring Boot 서버 주소 |
| `PRODUCT_ID` | `1`                                | 테스트 상품 ID (setup.sh 출력값 사용) |
| `STOCK` | `500`                              | race.js - 기대 재고 수 |
| `USER_COUNT` | `1000`                             | race.js - 동시 사용자 수 |

---

## 결과 해석

| 지표 | 정상 | 주의 | 위험 |
|------|------|------|------|
| p95 응답시간 | < 500ms | 500ms ~ 2s | > 2s |
| 에러율 (5xx) | 0% | < 1% | > 1% |
| Oversell | 0건 | - | 1건 이상 |
| Consumer Lag | 0 ~ 10 | 10 ~ 100 | > 100 |

Consumer Lag은 Kafka UI(`http://localhost:8080`) 또는 Grafana(`http://localhost:3000`)에서 확인하세요.

---

## 주의사항

- **데이터 재사용 불가**: 앱 재시작 시 테이블이 초기화됩니다. 재시작할 때마다 `setup.sh`를 다시 실행하세요.
- **race.js 1회성**: 재고 소진 후 재실행하려면 `setup.sh`로 데이터를 리셋하세요.
- **host.docker.internal**: Docker Desktop(Windows/Mac)에서는 자동 동작합니다. Linux에서는 docker-compose.yaml의 `extra_hosts` 설정으로 처리됩니다.