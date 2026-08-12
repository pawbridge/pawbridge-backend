# 00. PawBridge 백엔드 전체 지도

> **PawBridge** — 유기동물 입양 중개 + 커뮤니티 + 커머스를 한 플랫폼에 담은 Spring Cloud 기반 MSA.
>
> 이 문서 묶음은 **처음 이 레포를 여는 사람**을 위한 것입니다.
> 각 서비스가 무엇을 하고, 왜 그렇게 만들었고, 여기서 쓰인 어려운 기술이 무엇인지를
> **실제 코드를 인용해** 설명합니다. 문제로 보이는 지점은 근거와 함께 표시했습니다.

---

## 문서 색인

| 문서 | 다루는 것 | 여기서 배울 핵심 |
|---|---|---|
| **[01. discovery & config](01-discovery-config.md)** | Eureka, Config Server | 서비스 디스커버리, 설정 외부화, `{cipher}` 암호화, fail-fast |
| **[02. api-gateway](02-api-gateway.md)** | 단일 진입점, 중앙 인가 | Predicate/Filter, WebFlux `Mono`, JWT→헤더 번역, CORS |
| **[03. user-service](03-user-service.md)** | 인증·이메일·OAuth2·찜 | BCrypt, Redis Lua 원자성, JWT 2토큰, **Outbox 패턴**, SAGA |
| **[04. animal-service](04-animal-service.md)** | 공공데이터 배치, 검색 | **Spring Batch**, nori 형태소 분석, CQRS, **올바른 Kafka 소비자** |
| **[05. community-service](05-community-service.md)** | 게시글·댓글 | Soft Delete, JSON 컬럼, 멱등성 순서, **N+1 HTTP 호출** |
| **[06. store-service](06-store-service.md)** | 상품·재고·주문 | **Redisson 분산 락**, Cache-Aside, Jitter TTL, Write-Behind |
| **[07. payment-service](07-payment-service.md)** | 토스 결제 | 인증/승인 분리, **금액 교차 검증**, 결제 멱등성, 보상 취소 |
| **[08. infrastructure](08-infrastructure.md)** | Kafka·Debezium·ES·배포 | **CDC와 EventRouter SMT**, 5노드 배치, Prometheus/Zipkin, CI/CD |

### 어디서 시작할까

**목적에 따라 읽는 순서가 다릅니다.**

```
처음 합류해서 전체를 파악하려면
   00 (이 문서) → 02 (요청이 어디로 가는지) → 03 (가장 큰 서비스) → 08 (어디서 도는지)

이벤트 아키텍처를 이해하려면        ★ 이 레포의 핵심
   03의 8절 (Outbox 발행) → 08의 4절 (Debezium 변환) → 04의 5절 (소비와 보상)

기능 하나를 고치려면
   해당 서비스 문서만 읽어도 충분합니다 (각 문서는 독립적으로 읽힙니다)

면접·발표를 준비하려면
   03의 8절, 06의 4~6절, 04의 2절, 08의 4절 — 이 네 곳이 가장 차별화됩니다
```

---

## 1. 한 장으로 보는 시스템

```
                          브라우저 / 프론트엔드
                                   │ HTTPS
                    ┌──────────────▼──────────────┐
                    │  nginx  (TLS 종료, node-1)   │
                    └──────────────┬──────────────┘
                                   │ HTTP
                    ┌──────────────▼──────────────────────────────┐
                    │  api-gateway                                │
                    │   · 쿠키에서 JWT 꺼내 검증                    │
                    │   · Role 기반 인가                           │
                    │   · X-User-Id / Role 헤더 주입               │
                    └──┬────────┬────────┬────────┬────────┬──────┘
       lb:// (Eureka)   │        │        │        │        │
        ┌───────────────▼┐ ┌─────▼────┐ ┌─▼───────┐ ┌▼──────┐ ┌▼────────┐
        │ user-service   │ │ animal   │ │community│ │ store │ │ payment │
        │ 인증·찜·마이페이지│ │ 배치·검색 │ │ 게시글   │ │ 주문   │ │ 토스     │
        └───┬────────┬───┘ └────┬─────┘ └────┬────┘ └───┬───┘ └────┬────┘
            │        │          │            │          │          │
            │        └──Feign───┴────────────┴──────────┘          │
            │           (동기 호출: 마이페이지 조합, 금액 검증)        │
            │                                                      │
        ┌───▼──────────────────────────────────────────────────────▼───┐
        │  MySQL (node-2, 단일 인스턴스 / 스키마 5개)                    │
        │    각 스키마마다 outbox 테이블                                 │
        └───────────────────────┬──────────────────────────────────────┘
                                │ binlog
                    ┌───────────▼────────────┐
                    │ Debezium (Kafka Connect)│  ← EventRouter SMT
                    └───────────┬────────────┘
                                │
                    ┌───────────▼────────────┐        ┌──────────────────┐
                    │  Kafka (node-3)         │───────►│ Elasticsearch    │
                    │  user.favorite.events   │  Sink  │ (nori, node-5)   │
                    │  user.compensation.events│       │  animals / posts │
                    │  community.post.events  │        └──────────────────┘
                    │  payment.events         │
                    │  store.outbox.events    │
                    └───────────┬─────────────┘
                                │ 소비
                    ┌───────────▼─────────────────────────────┐
                    │ animal(찜카운트) user(보상)               │
                    │ community(색인)  store(주문확정)          │
                    └─────────────────────────────────────────┘

        관측:  Micrometer → Prometheus → Grafana  /  Trace ID → Zipkin  (node-1)
```

---

## 2. 모듈 지도

| 모듈 | 역할 | Java 파일 | 포트 | 노드 | 상세 |
|---|---|---|---|---|---|
| `discovery-service` | Eureka 서버 (이름 → 주소) | 2 | 8761 | 1 | [01](01-discovery-config.md) |
| `config-service` | 설정 배급 (별도 git 레포에서 읽음) | 3 | 8888 | 2 | [01](01-discovery-config.md) |
| `api-gateway` | 단일 입구 + 중앙 인가 | 6 | 8080 | 1 | [02](02-api-gateway.md) |
| `user-service` | 회원·인증·이메일·OAuth2·찜·마이페이지·관리자 | **115** | 8081 | 4 | [03](03-user-service.md) |
| `store-service` | 상품·SKU·옵션·장바구니·주문·찜·검색 | **109** | 8085 | 4 | [06](06-store-service.md) |
| `animal-service` | 동물·보호소·공공API 배치·검색·S3 | **95** | 9020 | 3 | [04](04-animal-service.md) |
| `community-service` | 게시글·댓글·검색·S3 | 55 | 8089 | 4 | [05](05-community-service.md) |
| `payment-service` | 토스 결제 연동 | 18 | 8084 | 4 | [07](07-payment-service.md) |
| `integration-service` | — (껍데기, [8절](#8-integration-service는-죽은-모듈이다)) | 2 | — | — | [08](08-infrastructure.md#9-integration-service--죽은-모듈) |

### 기술 스택

| 분류 | 기술 |
|---|---|
| 언어/프레임워크 | Java 17, Spring Boot 3.4.11 (config만 3.4.12) |
| MSA | Spring Cloud 2024.0.2 (animal·store·payment는 2024.0.0), Gateway, Eureka, Config, OpenFeign |
| 인증 | Spring Security, JJWT 0.12.5, BCrypt, OAuth2 Client(Google) |
| 영속성 | Spring Data JPA, MySQL 8.0 |
| 캐시·락 | Redis, Redisson 3.45 (분산 락·RMap·RSet) |
| 메시징 | Kafka 7.5.0, Debezium MySQL Connector 2.2.1 (CDC) |
| 검색 | Elasticsearch 7.17 + analysis-nori |
| 배치 | Spring Batch |
| 저장소 | AWS S3 (awspring 3.2.0, community만 3.0.3) |
| 관측 | Micrometer, Prometheus, Grafana, Zipkin(Brave) |
| 배포 | Docker Compose × EC2 5대, GitHub Actions 10개 |

---

## 3. 모노레포인데 하나의 빌드가 아니다

이 구조를 먼저 이해해야 헷갈리지 않습니다.

```
pawbridge-backend/
├── (루트에 settings.gradle / build.gradle 이 없다)   ★
├── user-service/
│   ├── settings.gradle    ← 각자 독립 Gradle 프로젝트
│   ├── build.gradle
│   ├── gradlew            ← 각자 래퍼
│   └── Dockerfile
├── animal-service/  (같은 구조)
└── ...
```

**즉 폴더만 같이 있고 빌드·의존성은 완전히 독립입니다.**

```bash
# 빌드는 각 폴더에서
cd user-service && ./gradlew clean build

# 루트에서 전체 빌드하는 명령은 없다
```

CI도 이 구조를 따릅니다([08의 8절](08-infrastructure.md#8-cicd--서비스별-독립-배포)).

```yaml
- name: Build User Service
  run: |
    cd user-service        # ★ 폴더로 들어가서
    ./gradlew clean build -x test
```

**장점**: 서비스마다 스프링 버전·의존성을 따로 올릴 수 있고, 폴더째로 떼어 별도 레포로 옮기기 쉽습니다.
**단점**: 공통 코드를 공유할 수단이 없습니다 → [4절](#4-공유-부품이-없다는-것의-대가)의 문제로 이어집니다.

### ⚠️ 설정은 이 레포에 없습니다

```
pawbridge-backend/        ← 코드
pawbridge-config-repo/    ← 설정 (별도 git 레포)  ★
```

**실제 배포에 적용되는 설정은 `pawbridge-config-repo`에 있습니다.**
Config Server가 준 설정이 앱 내부 `application.yml`보다 **우선순위가 높기** 때문입니다.

```
pawbridge-config-repo/
├── application.yml            전 서비스 공통 (JPA, actuator, 로깅)
├── application-dev.yml        전 서비스 + dev (Kafka·Redis·Eureka·Zipkin 주소)
├── api-gateway-dev.yml        ★ 게이트웨이 라우팅 규칙이 여기 있다
├── user-service-dev.yml       DB·메일·OAuth·JWT ({cipher} 암호화)
└── ...
```

> 📌 **가장 많이 헤매는 지점**: `api-gateway/src/main/resources/application.yml`의 라우팅을 고쳐도
> 배포하면 안 바뀝니다. 고칠 곳은 config repo입니다.
> 두 파일의 차이는 [02의 7절](02-api-gateway.md#7-함정-코드의-라우팅-규칙은-실제로-쓰이지-않는다)에 표로 정리했습니다.

---

## 4. 공유 부품이 없다는 것의 대가

MSA에서 코드를 공유하지 않는 것은 **정당한 선택**입니다. 독립 배포를 지키기 위한 것입니다.
그런데 이 레포에서는 "공유하지 않기로 했다"가 아니라 **"복붙했다"** 에 가까운 상태입니다.

### 같은 파일이 여러 서비스에 복제되어 있습니다

| 복제된 것 | 어디에 |
|---|---|
| `ResponseDTO`, `CustomResponseUtil` | user, community |
| `ApplicationException` + `ErrorCode` + `GlobalExceptionRestAdvice` | user, community |
| `OutboxEvent` / `Outbox` 엔티티 + 서비스 | user, animal, community, store, payment (**5개**) |
| `ProcessedEvent` 엔티티 | user, animal, community |
| Outbox/ProcessedEvent 정리 스케줄러 | user, community |
| Kafka Consumer 설정 | user, animal, community |
| S3 업로드 서비스 | animal, community, store |

### 그 결과 ① — 버전이 어긋납니다

```
Spring Boot     3.4.11  (config-service만 3.4.12)
Spring Cloud    2024.0.2  /  2024.0.0 (animal, store, payment)
awspring S3     3.2.0     /  3.0.3 (community)
```

### 그 결과 ② — 같은 패턴이 서비스마다 다르게 구현되고, 그중 하나가 틀립니다

**Outbox 트랜잭션 전파**

| 서비스 | 설정 | 판정 |
|---|---|---|
| user-service | `@Transactional(propagation = REQUIRES_NEW)` | ❌ **원자성이 깨진다** |
| animal-service | `@Transactional` | ✅ |
| community-service | `@Transactional` | ✅ |

→ [03의 8-5절](03-user-service.md#8-5--여기에-결함이-있습니다--requires_new가-원자성을-깬다)

**Kafka 소비자 실패 처리**

| 서비스 | 실패 시 | 판정 |
|---|---|---|
| animal-service | throw → 3회 재시도 → 보상 이벤트 발행 | ✅ 모범 |
| community-service | throw → 3회 재시도 → 로그 (재동기화 배치가 보완) | ✅ 근거 있음 |
| user-service (보상) | catch → **ack** → 유실 | ❌ |
| store-service (결제) | catch → **ack** → 유실 | 🚨 **돈이 걸려 있다** |

→ [04의 5절](04-animal-service.md#5--kafka-소비자--이-프로젝트에서-가장-잘-만든-부분) vs
[06의 7-3절](06-store-service.md#7-3--결제-이벤트가-조용히-유실된다)

**`TRUSTED_PACKAGES`**

| 서비스 | 값 | 판정 |
|---|---|---|
| animal-service | `"com.pawbridge.*"` | ✅ |
| user-service | `"*"` | ⚠️ 역직렬화 공격 표면 |

### 최소한의 개선

전부 공유할 필요는 없습니다. **정확성이 걸린 것만** 얇은 라이브러리로 뽑으면 충분합니다.

```
pawbridge-common/               (별도 레포 또는 루트에 Gradle 모듈)
├── outbox/       OutboxEvent, OutboxService, ProcessedEvent   ★ 가장 중요
├── kafka/        ConsumerConfig, ErrorHandler 프리셋
└── web/          ResponseDTO, ErrorCode, GlobalExceptionAdvice
```

**Outbox만이라도 공유하면** 위 세 표의 문제가 한 번에 사라집니다.

---

## 5. 반복되는 실수 하나 — "트랜잭션 밖의 작업을 커밋 전에 한다"

이 레포에서 **같은 형태의 오류가 네 곳**에 있습니다. 하나만 이해하면 나머지가 다 보입니다.

**문제의 형태**

```
@Transactional {
    ...DB 변경...                  ← 아직 커밋되지 않았다
    외부 시스템에 되돌릴 수 없는 작업  ← 그런데 이미 실행됐다
}                                   ← 여기서 커밋 (또는 롤백)
```

**네 곳**

| 위치 | 무엇을 커밋 전에 했나 | 결과 |
|---|---|---|
| [06 store — 재고 차감](06-store-service.md#4-4--락이-트랜잭션보다-먼저-풀린다--초과-판매를-막지-못한다) | **분산 락 해제** | 다른 스레드가 옛 재고를 읽어 **초과 판매** |
| [06 store — 장바구니 동기화](06-store-service.md#6-4--그런데-여기-심각한-문제가-있습니다) | **Redis dirty 플래그 제거** | 롤백 시 장바구니 변경이 **영구 유실** |
| [05 community — 게시글 수정](05-community-service.md#6-1--이미지-수정-순서--삭제가-업로드보다-먼저다) | **S3 파일 삭제** | 업로드 실패 시 기존 이미지 **영구 소실** |
| [07 payment — 보상 기록](07-payment-service.md#4--보상-트랜잭션이-스스로를-지운다) | (반대) **outbox INSERT 후 throw** | 롤백으로 보상 이벤트가 **사라짐** |

**기억할 규칙 두 개**

```
① 되돌릴 수 없는 외부 작업(파일 삭제, 락 해제, 메일 발송)은 커밋 이후에 한다
    → TransactionSynchronization.afterCommit() 또는 트랜잭션 밖으로 이동

② 반드시 남아야 하는 기록(실패 로그, 보상 요청)은 부모 트랜잭션과 운명을 분리한다
    → 별도 빈 + @Transactional(propagation = REQUIRES_NEW)
```

> 흥미로운 점: `REQUIRES_NEW`가 [user-service에서는 오답](03-user-service.md#8-5--여기에-결함이-있습니다--requires_new가-원자성을-깬다)이고
> [payment-service에서는 정답](07-payment-service.md#고치는-방법)입니다.
> **판단 기준은 "이 기록이 비즈니스 변경과 운명을 같이해야 하는가"** 입니다.
> 함께 커밋돼야 하면 `REQUIRED`, 롤백돼도 남아야 하면 `REQUIRES_NEW`.

---

## 6. 반복되는 실수 둘 — 경로가 두 체계로 갈렸다

**프론트엔드는 `/api/...`로 부르고, 서비스는 `/api/v1/...`로 받습니다.**
그 간극을 게이트웨이의 `RewritePath`가 메웁니다.

```
프론트:  POST /api/products
게이트웨이: RewritePath  /api/(.*)  →  /api/v1/$1
서비스:  POST /api/v1/products      ← @RequestMapping("/api/v1/products")
```

**이 이중 체계가 세 가지 문제를 만들었습니다.**

### ① 인가 규칙이 매칭되지 않아 실제로 뚫려 있습니다

게이트웨이의 인가 목록은 문자열 비교입니다. 그런데 **필터가 리라이트 전 경로를 보는지 후 경로를 보는지가
라우트마다 다릅니다**(필터 선언 순서 때문).

```java
ADMIN_ONLY_PATHS = List.of("POST:/api/v1/shelters", "POST:/api/products", ...);
//                                  └ v1 있음          └ v1 없음
```

| 요청 | 필터가 보는 경로 | 목록의 패턴 | 결과 |
|---|---|---|---|
| `POST /api/shelters` | `/api/shelters` (리라이트 전) | `/api/v1/shelters` | ❌ **일반 회원이 보호소 등록 가능** |
| `POST /api/products` | `/api/v1/products` (리라이트 후) | `/api/products` | ❌ **일반 회원이 상품 등록 가능** |
| `GET /api/admin/orders` | `/api/v1/admin/orders` | `/api/v1/admin/**` (접두사) | ✅ 정상 |

→ 자세한 추적: [02의 6-2절](02-api-gateway.md#6-2-인가-목록과-경로-리라이트가-엇갈려서-생긴-실제-구멍)

### ② 서비스 간 Feign 호출이 404를 맞습니다

**Feign 호출은 게이트웨이를 거치지 않으므로 리라이트가 없습니다.**
프론트 기준 경로를 그대로 쓰면 어긋납니다.

```java
// payment-service → store-service
@GetMapping("/api/orders/uuid/{orderUuid}")      // ← 프론트 기준 경로
// store-service 실제:  /api/v1/orders/uuid/{orderUuid}
```

→ [07의 2절](07-payment-service.md#-그런데-이-검증-호출이-404를-맞습니다).
이 호출이 **결제 금액 검증**이므로 영향이 큽니다.

### ③ 리라이트가 이중으로 적용되는 경로가 있습니다

```
RewritePath=/api/(.*)  →  /api/v1/$1
입력이 이미 /api/v1/animals 면?  →  /api/v1/v1/animals   ← 404
```

### 근본 해법

**프론트도 `/api/v1/`을 쓰게 통일하면** `RewritePath`가 전부 사라지고 위 세 문제가 **구조적으로**
발생하지 않게 됩니다. 그전까지는 인가를 `/api/v1/admin/**`처럼 **접두사 규칙**으로 표현하는 것이
목록 관리보다 안전합니다(그 규칙만 실제로 잘 동작하고 있습니다).

---

## 7. 테스트가 사실상 없다

```
테스트 파일 9개 = 서비스 9개 × contextLoads() 스켈레톤 1개
비즈니스 로직 테스트: 0건
```

**이 레포의 가장 어려운 코드가 전부 검증되지 않은 상태입니다.**

- Outbox 원자성 (트랜잭션 전파)
- 소비자 멱등성 (중복 이벤트)
- 재고 동시성 (분산 락)
- 결제 멱등성 (중복 승인)
- 배치 실패 처리 (skip 정책)

그리고 실제로 **이 문서들이 지적한 결함 대부분이 정확히 그 자리에서 나왔습니다.**

### 처음 쓸 테스트 5개 (효과 순)

```java
// ① Outbox 원자성 — 03의 8-5 결함을 잡는다
@Test void 찜_트랜잭션이_롤백되면_outbox에도_남지_않는다() {
    assertThrows(RuntimeException.class, () -> favoriteService.찜하고_강제로_실패());
    assertThat(outboxEventRepository.count()).isZero();      // 현재 코드는 실패한다
}

// ② 결제 실패 이벤트 보존 — 07의 4절 결함을 잡는다
@Test void DB저장이_실패하면_PAYMENT_FAILED_outbox가_남는다() { ... }

// ③ 재고 동시성 — 06의 4-4 결함을 잡는다
@Test void 재고1개에_동시주문_2건이면_한_건만_성공한다() throws Exception {
    // CountDownLatch 로 두 스레드를 동시에 출발시킨다
}

// ④ 소비자 멱등성 — 정상 동작을 고정한다
@Test void 같은_eventId를_두_번_처리해도_찜카운트는_1만_증가한다() { ... }

// ⑤ 리프레시 토큰 유일성 — 03의 4절 결함을 잡는다
@Test void 같은_초에_두_사용자가_로그인해도_토큰이_겹치지_않는다() { ... }
```

**①만 써도 충분히 가치가 있습니다.** 이 레포의 핵심 패턴의 핵심 성질을 검증하고,
**현재 코드는 이 테스트에서 실패합니다.**

그리고 CI에서 `-x test`를 제거해야 합니다([08의 8-4절](08-infrastructure.md#8-4--배포-파이프라인의-문제-세-가지)).
CI에서 돌지 않는 테스트는 곧 아무도 실행하지 않게 됩니다.

---

## 8. 잘 만들어진 것 (그대로 유지할 것)

문제만 나열하면 균형이 맞지 않습니다. **실제로 수준이 높은 부분**을 정리합니다.

| 항목 | 왜 좋은가 | 위치 |
|---|---|---|
| **Transactional Outbox + Debezium CDC** | Dual Write 문제를 정공법으로 해결. 애플리케이션이 Kafka를 모른다 | [03의 8-3](03-user-service.md#8-3-해법-transactional-outbox-패턴) |
| **animal-service의 Kafka 소비자** | Delegator 패턴, eventId 필수 검증, 수동 커밋, 재시도→보상. 교과서적 | [04의 5절](04-animal-service.md#5--kafka-소비자--이-프로젝트에서-가장-잘-만든-부분) |
| **"보상하지 않는다"는 판단** | 찜 취소 실패는 보상하면 사용자 의도를 거스른다 — 근거를 주석에 남겼다 | [04의 5-7](04-animal-service.md#5-7-favorite_removed는-보상하지-않는다--판단의-근거) |
| **FATAL-ERROR 로그** | 보상마저 실패한 경우를 예상하고, 로그에 "할 일 4단계"를 적었다 | [04의 5-8](04-animal-service.md#5-8-fatal-error-처리--보상마저-실패했을-때) |
| **결제 금액 교차 검증** | 클라이언트가 보낸 금액을 믿지 않는다. 실무에서 가장 흔한 사고를 막았다 | [07의 2절](07-payment-service.md#-안전장치-2--금액-위조-방지-가장-중요한-코드) |
| **결제 멱등성 + S008 처리** | 중복 요청에 에러가 아니라 지난 결과를 반환. PG 중복 코드까지 해석 | [07의 3절](07-payment-service.md#-안전장치-1--멱등성-같은-결제를-두-번-승인하지-않기) |
| **주문 스냅샷** | 영수증은 나중에 변해서는 안 된다. 이름·가격을 복사해 보존 | [06의 3절](06-store-service.md#3-주문-스냅샷--과거를-보존하는-설계) |
| **Cache-Aside + Double-Check + Jitter TTL** | 캐시 스탬피드 방지. 학습 프로젝트에서 보기 드문 수준 | [06의 5절](06-store-service.md#-cache-aside--캐시-스탬피드-방지) |
| **Redis Lua 원자적 rate limiting** | 서버가 늘어도 제한이 정확하다 | [03의 2절](03-user-service.md#-lua-스크립트--확인하고-증가를-하나로-묶기) |
| **HttpOnly 쿠키로 전환** | localStorage의 XSS 탈취 경로를 차단 | [03의 5절](03-user-service.md#5-쿠키-전략--localstorage에서-옮겨온-이유) |
| **APMS 배치의 필드 소유권 분리** | 배치가 사용자 데이터(찜 수, 보호소 글)를 덮어쓰지 않게 막았다 | [04의 2-7](04-animal-service.md#2-7-upsert--있으면-수정-없으면-생성) |
| **외부 상태 vs 자체 상태 분리** | `apmsProcessState`와 `status`를 나눴다. 외부 연동의 정석 | [04의 2-7](04-animal-service.md#2-7-upsert--있으면-수정-없으면-생성) |
| **nori 사용자 사전** | "믹스견", "웰시코기" 같은 도메인 단어를 지켜냈다 | [04의 4-2](04-animal-service.md#4-2-사용자-사전--도메인-단어를-지켜내기) |
| **CQRS (쓰기 MySQL / 읽기 ES)** | 화면 성격에 따라 저장소를 골랐다. 상세만 MySQL | [04의 3절](04-animal-service.md#3-cqrs와-facade--읽기와-쓰기를-다른-저장소로) |
| **email-service 역방향 통합** | "함께 변경되는 것은 함께 둔다" — 근거 있는 후퇴 | [03의 7절](03-user-service.md#7-역방향-리팩토링--email-service를-왜-합쳤나) |
| **`{cipher}` 설정 암호화** | 비밀값을 git에 안전하게 보관 | [01의 2절](01-discovery-config.md#cipher--비밀값을-git에-올리는-방법) |
| **`fail-fast` + `retry`** | 조용한 오작동 대신 시끄러운 실패, 시작 순서는 재시도로 | [01의 2절](01-discovery-config.md#fail-fast와-retry--시작-순서-문제를-푸는-방법) |
| **`envsubst` 치환 배포** | 사설 IP를 git에 넣지 않고 설정에 넣는다 | [08의 7-3](08-infrastructure.md#7-3-dev용과-prod용-prometheusyml이-다릅니다) |
| **경로 필터 CI** | 모노레포에서 바뀐 서비스만 배포 | [08의 8-2](08-infrastructure.md#8-2-경로-필터--바뀐-것만-배포한다) |
| **분산 추적 전면 적용** | 6개 서비스 모두 Micrometer Tracing. Zipkin에서 한 화면에 보인다 | [08의 7-1](08-infrastructure.md#7-1-세-도구의-역할이-다릅니다) |

---

## 9. 전체 개선 우선순위

각 문서의 목록을 합쳐 **심각도 순**으로 정렬했습니다.

### 🚨 지금 데이터·돈이 틀어지고 있는 것

| # | 항목 | 서비스 | 문서 |
|---|---|---|---|
| 1 | 결제 이벤트를 예외 삼키고 ack → **결제 완료가 주문에 반영되지 않고 유실** | store | [06 7-3](06-store-service.md#7-3--결제-이벤트가-조용히-유실된다) |
| 2 | 분산 락이 커밋 전에 풀려 **재고 초과 판매** | store | [06 4-4](06-store-service.md#4-4--락이-트랜잭션보다-먼저-풀린다--초과-판매를-막지-못한다) |
| 3 | 금액 검증 Feign 경로가 404 → **결제 확인 자체가 실패** | payment | [07 2](07-payment-service.md#-그런데-이-검증-호출이-404를-맞습니다) |
| 4 | 보상 outbox가 `throw`로 롤백 → **환불했는데 재고가 안 돌아온다** | payment | [07 4](07-payment-service.md#-보상-트랜잭션이-스스로를-지운다) |
| 5 | Outbox `REQUIRES_NEW` → **유령 이벤트** (찜 없는데 카운트 증가) | user | [03 8-5](03-user-service.md#8-5--여기에-결함이-있습니다--requires_new가-원자성을-깬다) |
| 6 | `createRefreshToken()`에 `jti` 없음 → **같은 초 로그인 시 토큰 충돌** | user | [03 4](03-user-service.md#-여기에-심각한-결함이-있습니다--같은-초에-로그인하면-토큰이-겹칩니다) |
| 7 | 게이트웨이 인가 목록 불일치 → **일반 회원이 상품·보호소 등록 가능** | gateway | [02 6-2](02-api-gateway.md#6-2-인가-목록과-경로-리라이트가-엇갈려서-생긴-실제-구멍) |

### ⚠️ 조용히 실패하거나 조만간 터질 것

| # | 항목 | 서비스 | 문서 |
|---|---|---|---|
| 8 | 배치 Reader가 API 실패를 삼켜 **"성공"으로 끝난다** | animal | [04 2-5](04-animal-service.md#2-5--reader의-예외-처리가-배치를-조용히-성공시킨다) |
| 9 | `.skip(Exception.class)` + Processor `null` → **실패가 집계되지 않는다** | animal | [04 2-9](04-animal-service.md#2-9--faulttolerant-설정이-실패를-감춘다) |
| 10 | 장바구니 동기화가 전원 한 트랜잭션 → **한 명 실패 = 전원 유실** | store | [06 6-4](06-store-service.md#6-4--그런데-여기-심각한-문제가-있습니다) |
| 11 | 이미지 수정이 삭제 먼저 → **업로드 실패 시 기존 이미지 소실** | community | [05 6-1](05-community-service.md#6-1--이미지-수정-순서--삭제가-업로드보다-먼저다) |
| 12 | 보상 이벤트 실패 시 ack → **마지막 방어선이 유실** | user | [03 8-9](03-user-service.md#8-9-보상-이벤트-소비자의-또-다른-문제--실패해도-ack) |
| 13 | `@EnableJpaAuditing` 누락 → **결제 기록에 시각이 없다** | payment | [07 6](07-payment-service.md#-enablejpaauditing이-없다--타임스탬프가-전부-null) |
| 14 | `@Transactional`이 외부 HTTP를 감싸 **커넥션 풀 고갈 위험** | payment | [07 5](07-payment-service.md#-트랜잭션이-외부-http-호출을-감싸고-있다) |
| 15 | 닉네임 조회 N+1 HTTP + 페이징 없음 | community | [05 5](05-community-service.md#-n1-http-호출--msa에서-가장-비싼-실수) |
| 16 | 커넥터 JSON에 `root/root` 자격증명 커밋 | infra | [08 4-6](08-infrastructure.md#4-6--커넥터-json에-db-자격증명이-하드코딩되어-git에-있습니다) |
| 17 | `animals` 인덱스에 **중복 문서 누적** (`key.ignore: true`) | infra | [08 4-4](08-infrastructure.md#4-4--animal-service의-es-동기화가-두-경로로-겹칩니다) |
| 18 | 서비스 포트 노출 + 헤더 무검증 신뢰 | gateway/infra | [02 6-1](02-api-gateway.md#6-1-게이트웨이-우회--헤더를-무조건-믿는다), [08 7-4](08-infrastructure.md#7-4--포트를-노출한-진짜-이유) |
| 19 | 쿠키 `Secure` 누락 (프로필이 `dev`라 prod 분기가 죽어 있다) | user | [03 5](03-user-service.md#-문제-prod-분기가-실제로는-실행되지-않습니다) |
| 20 | `:latest` 태그만 사용 → **롤백 불가** | infra | [08 8-4](08-infrastructure.md#8-4--배포-파이프라인의-문제-세-가지) |

### 구조 개선 (급하지 않지만 반복 실수를 막는다)

| # | 항목 | 문서 |
|---|---|---|
| 21 | Outbox를 공통 모듈로 분리 | [4절](#4-공유-부품이-없다는-것의-대가) |
| 22 | 경로를 `/api/v1/`로 통일하고 `RewritePath` 제거 | [6절](#6-반복되는-실수-둘--경로가-두-체계로-갈렸다) |
| 23 | 인가를 목록 대신 접두사 규칙으로 | [02 6-2](02-api-gateway.md#고치는-방법) |
| 24 | 커넥터 설정 통일 (`schemas.enable`, `event.id`) | [08 4-5](08-infrastructure.md#4-5--커넥터-설정이-서비스마다-다릅니다) |
| 25 | `ddl-auto: update` → Flyway 마이그레이션 | [08 6](08-infrastructure.md#6-mysql--비용-절감을-명시한-단일-인스턴스) |
| 26 | `.env` 생성을 Config Server로 통합 | [08 8-4](08-infrastructure.md#8-4--배포-파이프라인의-문제-세-가지) |
| 27 | actuator를 별도 포트로 분리 | [08 7-4](08-infrastructure.md#7-4--포트를-노출한-진짜-이유) |
| 28 | `integration-service` 제거 또는 문서화 | [08 9](08-infrastructure.md#9-integration-service--죽은-모듈) |
| 29 | 테스트 5개 작성 + CI에서 `-x test` 제거 | [7절](#7-테스트가-사실상-없다) |

**손볼 순서를 하나만 고른다면**: **1 → 2 → 3 → 4 → 그 넷을 지키는 테스트**입니다.
전부 돈·재고·데이터가 실제로 어긋나는 경로이고, 나머지는 그 뒤에 해도 늦지 않습니다.

---

## 10. 총평

**패턴 이해도가 눈에 띄게 높습니다.**
Transactional Outbox + Debezium CDC + SAGA 보상 + `ProcessedEvent` 멱등성 + 수동 오프셋 커밋을
조합한 찜 파이프라인, nori 사용자 사전까지 손본 한국어 검색, 분산 락·Cache-Aside·Jitter TTL·Write-Behind가
실제로 도는 커머스, 5노드 분산에 경로 필터 CI/CD와 Prometheus·Grafana·Zipkin까지 —
**이론으로 아는 수준이 아니라 실제로 돌려본 흔적**이 남아 있습니다.
주석에 "왜 이렇게 했는지"와 트레이드오프를 적어둔 곳이 많은 것도 좋은 신호입니다.

**약한 곳은 일관성과 검증입니다.**
잘 만든 패턴이 서비스마다 복제되면서 품질이 갈렸습니다.
찜 소비자는 모범인데 결제 소비자는 이벤트를 유실하고, Outbox는 세 서비스 중 하나가 원자성을 깼습니다.
그리고 그것을 잡아줄 테스트가 없어 **아무도 모르는 채 배포되어 있습니다.**

**즉 이 레포의 다음 한 걸음은 새 기능이 아니라 "이미 만든 것을 지키는 장치"입니다.**
공통 모듈 하나와 테스트 다섯 개면, 위 9절의 목록 절반이 다시 발생하지 않습니다.

---

## 부록 — 자주 찾는 파일 지도

| 찾는 것 | 파일 |
|---|---|
| 라우팅 규칙 (실제 적용) | `pawbridge-config-repo/api-gateway-dev.yml` |
| 라우팅 규칙 (로컬) | [api-gateway/src/main/resources/application.yml](../api-gateway/src/main/resources/application.yml) |
| JWT 검증·인가 | [api-gateway/.../JwtAuthorizationGatewayFilterFactory.java](../api-gateway/src/main/java/com/pawbridge/apigateway/filter/JwtAuthorizationGatewayFilterFactory.java) |
| JWT 발급 | [user-service/.../jwt/JwtProvider.java](../user-service/src/main/java/com/pawbridge/userservice/jwt/JwtProvider.java) |
| 쿠키 설정 | [user-service/.../util/CookieUtil.java](../user-service/src/main/java/com/pawbridge/userservice/util/CookieUtil.java) |
| Redis Lua rate limiting | [user-service/.../email/config/RedisConfig.java](../user-service/src/main/java/com/pawbridge/userservice/email/config/RedisConfig.java) |
| Outbox 발행 (모범) | [animal-service/.../service/OutboxService.java](../animal-service/src/main/java/com/pawbridge/animalservice/service/OutboxService.java) |
| Kafka 소비자 (모범) | [animal-service/.../consumer/FavoriteEventConsumer.java](../animal-service/src/main/java/com/pawbridge/animalservice/consumer/FavoriteEventConsumer.java) |
| 재시도·보상 설정 | [animal-service/.../config/KafkaConsumerConfig.java](../animal-service/src/main/java/com/pawbridge/animalservice/config/KafkaConsumerConfig.java) |
| Spring Batch Job | [animal-service/.../batch/job/ApmsAnimalBatchJob.java](../animal-service/src/main/java/com/pawbridge/animalservice/batch/job/ApmsAnimalBatchJob.java) |
| 분산 락 + 재고 | [store-service/.../order/service/OrderServiceImpl.java](../store-service/src/main/java/com/pawbridge/storeservice/domain/order/service/OrderServiceImpl.java) |
| 캐시 스탬피드 방지 | [store-service/.../product/facade/ProductFacade.java](../store-service/src/main/java/com/pawbridge/storeservice/domain/product/facade/ProductFacade.java) |
| 결제 로직 전체 | [payment-service/.../PaymentServiceImpl.java](../payment-service/src/main/java/com/pawbridge/paymentservice/domain/payment/service/PaymentServiceImpl.java) |
| Debezium 커넥터 | [infrastructure/kafka/connectors/](../infrastructure/kafka/connectors/) |
| ES 한국어 매핑 | [infrastructure/elasticsearch/mappings/animals-index-mapping.json](../infrastructure/elasticsearch/mappings/animals-index-mapping.json) |
| 노드별 배포 구성 | [deployment/](../deployment/) |
| CI/CD | [.github/workflows/](../.github/workflows/) |
