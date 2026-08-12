# PawBridge 백엔드 — 기여 내역 및 면접 대비 정리

> 작성 기준: `minimini1212` 커밋 79개 / **+11,893줄 / -1,495줄** (2025-11-06 ~ 2026-01-07)
> 담당 영역: **user-service + api-gateway 전체**, 그리고 community-service · store-service · animal-service의 인증 / 관리자 / 마이페이지 영역

---

## 1. 프로젝트 개요

**PawBridge** — 유기동물 입양 중개 + 커뮤니티 + 커머스 플랫폼

MSA 구성:
`api-gateway`, `discovery-service`(Eureka), `config-service`, `user-service`, `animal-service`, `community-service`, `store-service`, `payment-service`, `monitoring`

**내 역할: 인증/인가 도메인 오너 + Gateway 오너 + 전사 공통 규약 정립자**

---

## 2. 기여 영역 상세

### 2-1. 인증/인가 시스템 전체 설계·구현 (핵심 기여)

#### 구현 범위

| 기능 | 파일 |
|---|---|
| 회원가입 (이메일 인증 연동, Role/보호소번호 검증) | `UserServiceImpl.signUp()` |
| 로그인 (Security 필터 커스텀) | `filter/JwtAuthenticationFilter.java` |
| JWT 발급/검증 | `jwt/JwtProvider.java` |
| RefreshToken 재발급 + 로그아웃 | `service/AuthServiceImpl.java` |
| Google 소셜 로그인 | `oauth2/service/CustomOAuth2UserService.java`, `oauth2/handler/OAuth2SuccessHandler.java` |
| Gateway 인가 필터 | `api-gateway/filter/JwtAuthorizationGatewayFilterFactory.java` (264줄) |
| 쿠키 기반 토큰 전달 | `util/CookieUtil.java` |

#### ① `UsernamePasswordAuthenticationFilter` 상속 방식 채택

```java
public class JwtAuthenticationFilter extends UsernamePasswordAuthenticationFilter {
    // setFilterProcessesUrl("/api/v1/auth/login")
    // attemptAuthentication() → AuthenticationManager → PrincipalDetailsService
    // successfulAuthentication() → JWT 발급 + 쿠키 세팅
}
```

- **왜**: Controller에서 직접 `passwordEncoder.matches()`를 호출하는 방식 대신 Spring Security의 `AuthenticationManager` → `UserDetailsService` → `PasswordEncoder` 검증 파이프라인을 그대로 활용. 인증 실패 예외(`BadCredentialsException`, `UsernameNotFoundException`)를 프레임워크가 표준화해주고, OAuth2 로그인과 같은 `SecurityContext` 위에서 동작해 두 로그인 경로가 하나의 인증 모델을 공유.
- **효과**: 로컬 로그인과 소셜 로그인이 동일한 `PrincipalDetails`(= `UserDetails` + `OAuth2User` 동시 구현)를 반환 → `OAuth2SuccessHandler`에서 `jwtProvider.createAccessToken(user)`을 **그대로 재사용**. 토큰 발급 로직 중복 0.

#### ② 상태 저장 방식: Access Token은 Stateless, Refresh Token은 DB 저장

```java
// Access: 서명 검증만 (DB 조회 없음)
// Refresh: refresh_tokens 테이블에서 조회 + 만료 확인 + 회전(rotation)
refreshToken.updateToken(newRefreshToken, newExpiresAt);
```

- **왜**: Access Token을 DB/Redis에서 매 요청 조회하면 Stateless 이점이 사라짐. 반면 Refresh Token은 **탈취 시 강제 무효화(로그아웃)** 가 필요 → 서버 상태가 반드시 필요.
- **효과**: 일반 API 요청은 Gateway에서 **서명 검증만으로 통과**(DB I/O 0회), 로그아웃은 `deleteByUserId()` 한 번으로 즉시 무효화. 재발급 시 Refresh Token도 함께 교체(Rotation)해 재사용 공격 차단.

#### ③ Google OAuth2 — `provider` 컬럼으로 계정 분리

```java
@Table(uniqueConstraints = { @UniqueConstraint(columnNames = {"email", "provider"}) })
```

- **왜**: 같은 이메일로 로컬 가입과 구글 가입이 동시에 들어올 수 있음. `email` 단일 UNIQUE로 두면 둘 중 하나가 막히고, 제약을 아예 없애면 **계정 탈취(account linking) 취약점**이 생김.
- **처리**: `(email, provider)` 복합 UNIQUE + 구글 로그인 시 동일 이메일 LOCAL 계정 존재 여부를 명시적으로 검사해 `"이미 해당 이메일로 가입된 계정이 있습니다. 일반 로그인을 이용해주세요."` 안내.
- **추가 대응**: `authorization-uri`에 `?prompt=select_account`를 붙여 로그인 버튼 클릭 시 매번 계정 선택 화면이 뜨도록 수정 (커밋 `fc4a266`) — 기존엔 브라우저에 로그인된 계정으로 자동 진입해 다른 계정 로그인이 불가했던 실사용 버그.

#### ④ Gateway 중앙 인가 — 가장 설명하기 좋은 설계 판단

```java
// api-gateway/filter/JwtAuthorizationGatewayFilterFactory.java
WHITELIST         → 토큰 검증 스킵 (공개 조회 API, 로그인, 재발급)
ADMIN_ONLY_PATHS  → "POST:/api/products" 형태로 METHOD+PATH 매칭
NON_USER_PATHS    → ROLE_ADMIN, ROLE_SHELTER만 (동물 등록/수정/삭제)

// 검증 후 하위 서비스로 신원 전파
request.mutate()
    .header("X-User-Id", userId.toString())
    .header("X-User-Email", email)
    .header("X-User-Role", role)
    // ROLE_SHELTER면 X-Care-Reg-No 추가
```

- **왜**: 서비스가 6개인데 각자 JWT 파싱을 하면 ① `jwt.secret`이 모든 서비스에 퍼지고 ② 검증 로직이 6곳에 중복되고 ③ 정책 변경 시 6개를 동시 배포해야 함.
- **효과**: Gateway 한 곳에서만 서명 검증 → 하위 서비스는 `@RequestHeader("X-User-Id")`만 읽으면 됨. 실제로 `user-service`의 `SecurityConfig`는 `.anyRequest().permitAll()`로 두고 인가 책임을 완전히 Gateway로 위임.
- **`AntPathMatcher` 사용 이유**: `/api/v1/animals/*`처럼 와일드카드가 필요한데 `String.startsWith()`로는 `/api/v1/animals/expiring-soon`과 `/api/v1/animals/1/comments`를 구분할 수 없음. `METHOD:PATH` 조합 매칭으로 **"상품 조회는 공개, 등록은 ADMIN"** 같은 메서드별 정책을 표현.

> **면접 답변 포인트(트레이드오프 인지)**: Gateway를 우회해 서비스로 직접 요청이 들어오면 인가가 없음. 현재는 서비스가 내부 네트워크에만 노출된다는 전제이고, 프로덕션이라면 서비스 간 mTLS 또는 각 서비스에 내부망 전용 필터를 추가해야 한다고 답변. 실제로 `SecurityConfig`에 `// api gateway 에서 검증을 하므로 굳이 더 할 필요가 있나?` 라는 고민 주석이 남아있음.

#### ⑤ localStorage → HttpOnly Cookie 리팩토링 (커밋 `b9093d9`)

```java
// CookieUtil - 환경별 분기
if (isProduction) {
    cookieBuilder.append("; Domain=.pawbridge.kr");
    cookieBuilder.append("; SameSite=None");   // 크로스 사이트 전송 허용
    cookieBuilder.append("; Secure");           // HTTPS 전용
}
// 개발: SameSite 생략 (Lax 기본값, localhost는 Secure 없이 동작)
cookieBuilder.append("; HttpOnly");             // 공통: JS 접근 차단
```

- **문제**: 응답 body로 토큰을 내려 프론트가 `localStorage`에 저장 → **XSS 한 번이면 토큰 전량 탈취**. 게시글에 사용자 입력이 렌더링되는 커뮤니티 서비스가 있어 리스크가 실재.
- **선택**: `HttpOnly` 쿠키. JS가 읽을 수 없어 XSS로 토큰 유출 불가.
- **파생 문제와 해결**:
  1. **CSRF 노출** → JWT 사용으로 `csrf().disable()` 상태였으므로 `SameSite` 속성으로 방어. 프론트가 다른 도메인이라 `SameSite=Strict`는 쓸 수 없어 `None + Secure` 조합.
  2. **CORS** → `allowCredentials: true`가 필수. 이때 `allowedOrigins`에 `*`를 쓸 수 없어 `allowedOriginPatterns`로 명시적 화이트리스트 지정.
  3. **`Domain=.pawbridge.kr`** → 서브도메인(`api.` / `www.`) 간 쿠키 공유를 위해 상위 도메인으로 설정. 단 localhost에서는 도메인 불일치로 쿠키가 안 잡히므로 **프로필 기반 분기**(`spring.profiles.active`)로 처리.
  4. **Gateway 추출 로직 변경** → `Authorization: Bearer` 헤더 파싱을 `request.getCookies().get("accessToken")`으로 교체 (WebFlux `ServerHttpRequest` 방식).
- **효과**: 프론트에서 토큰 관리 코드 제거(쿠키는 브라우저가 자동 전송), XSS 토큰 탈취 경로 차단, 재발급도 쿠키만으로 동작 → **DTO 2개 삭제**(`RefreshTokenRequestDto`, `RefreshTokenResponseDto`).

---

### 2-2. 이메일 인증 — Redis + Lua 스크립트

**기능**: 회원가입 이메일 인증, 비밀번호 찾기(이메일 인증 후 재설정)

#### ① 인증 코드 저장소로 Redis 선택

```java
redisTemplate.opsForValue().set("email:code:" + email, code, 5, TimeUnit.MINUTES);
redisTemplate.opsForValue().set("email:verified:" + email, "true", 1, TimeUnit.HOURS);
```

- **왜**: 인증 코드는 5분 뒤 반드시 사라져야 하는 휘발성 데이터. RDB에 넣으면 만료 데이터를 지우는 배치/스케줄러가 필요하고 테이블이 계속 부풀어 오름. Redis의 **TTL이 만료 처리를 인프라 레벨에서 대신 해줌**.
- **효과**: 만료 로직 코드 0줄. `key : value : TTL` 3요소로 상태 관리 완결.

#### ② Rate Limiting을 Lua 스크립트로 — 원자성 확보

```lua
local count = redis.call('GET', KEYS[1])
if count and tonumber(count) >= tonumber(ARGV[1]) then
  return -1                                    -- 한도 초과
end
local newCount = redis.call('INCR', KEYS[1])
if newCount == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])       -- 첫 증가 시에만 TTL
end
return newCount
```

- **문제**: `GET` → 비교 → `INCR`를 애플리케이션 코드로 3번 왕복하면, 동시 요청 시 **모두 GET에서 한도 미달을 읽고 전부 통과**(Race Condition). 이메일 폭탄 발송이 가능.
- **왜 Lua**: Redis는 **싱글 스레드로 스크립트를 원자적 단위로 실행**. `GET`+비교+`INCR`+`EXPIRE`가 중간에 끼어들 틈 없이 처리됨. 분산 락(Redisson 등)을 도입하면 의존성·락 획득 비용·데드락 리스크가 생기는데, 이 케이스는 단일 키 연산이라 Lua로 충분.
- **`newCount == 1` 조건의 이유**: 매번 `EXPIRE`를 걸면 요청이 계속 들어오는 동안 TTL이 무한 갱신되어 **제한 창(window)이 리셋되지 않음**. 첫 증가에만 TTL을 설정해 고정 윈도우를 보장.
- **효과**: 발송 5회/5분, 검증 시도 5회 제한이 동시성 환경에서도 정확히 지켜짐 → 메일 발송 비용 및 무차별 코드 대입(brute-force) 차단.

#### ③ 이메일 발송 → Redis 저장 순서

```java
emailSenderService.sendVerificationEmail(email, code);  // 먼저 발송
redisTemplate.opsForValue().set(codeKey, code, ...);    // 성공 후 저장
```

- **왜**: 순서를 뒤집으면 발송 실패 시 "코드는 서버에 있는데 사용자는 못 받은" 상태가 되어 5분간 재발송이 막힘. 발송을 먼저 하면 예외 발생 시 Redis에 아무것도 남지 않아 즉시 재시도 가능.

#### ④ 계정 존재 여부 비노출 (Enumeration 방지)

```java
if (userOpt.isPresent()) { /* 발송 */ }
// 이메일이 없어도 동일하게 성공 응답 (보안)
```

- 비밀번호 재설정 요청 시 미가입 이메일이어도 200을 반환. **응답 차이로 가입 여부를 탐지하는 공격**을 막기 위함. 발송 실패도 catch해 성공 응답으로 통일.

#### ⑤ 아키텍처 결정: 독립 `email-service`를 `user-service`로 통합 (커밋 `f00e007`, -1,300줄)

- **문제**: 이메일 인증은 회원가입/비밀번호 재설정의 **동기적 선행 조건**인데, 별도 서비스로 두니 `user-service`가 `EmailServiceClient`(Feign)로 매번 동기 호출 → **네트워크 홉 추가 + 장애 지점 증가 + 두 서비스가 같은 Redis를 공유**하는 결합 상태.
- **판단**: 트랜잭션 경계와 데이터(Redis 키)를 공유하는 두 컴포넌트는 같은 서비스여야 한다. → `user-service/email/` 패키지로 흡수하고 서비스·Feign 클라이언트·별도 Gradle 모듈 전부 삭제.
- **효과**: 배포 단위 1개 감소, Feign 홉 제거, `EmailServiceUnavailableException` 처리 분기 소멸. 대신 `RedisConnectionFailureException` 핸들러를 `GlobalExceptionRestAdvice`에 추가해 인증 서비스 장애를 503으로 명확히 응답.

> **면접 포인트**: "MSA는 무조건 쪼개는 게 아니라, **함께 변하고 데이터를 공유하는 것은 함께 둔다**는 원칙으로 역방향 리팩토링을 결정했다"

---

### 2-3. 찜(Favorite) 기능 — Outbox Pattern + Debezium CDC + SAGA 보상 트랜잭션

가장 기술적으로 깊은 기여 (커밋 `41a6cfd`, +1,069줄).

#### 문제 정의

`user-service`가 찜을 저장하고 → `animal-service`의 찜 카운트를 증가시켜야 함. 서로 다른 DB이므로 **분산 트랜잭션 문제**.

#### 왜 단순 방식들을 쓰지 않았는가

| 방식 | 문제 |
|---|---|
| Feign 동기 호출 | animal-service 장애 시 찜 기능 전체 마비, 응답 지연 전파 |
| `save()` 후 `kafkaTemplate.send()` | **Dual Write 문제** — DB 커밋 성공 + Kafka 발행 실패 시 이벤트 영구 유실 (또는 그 반대) |
| 2PC (XA) | MySQL+Kafka 조합에서 사실상 불가, 성능/가용성 저하 |

#### 채택: Transactional Outbox Pattern + CDC

```java
// 1) 비즈니스 데이터와 이벤트를 같은 DB에 저장 → 원자성 확보
Favorite saved = favoriteRepository.save(favorite);
outboxService.saveEvent("Favorite", userId.toString(), "FAVORITE_ADDED",
                        "user.favorite.events", eventPayload);
```

```json
// 2) Debezium이 outbox_events 테이블의 binlog를 읽어 Kafka로 발행
"connector.class": "io.debezium.connector.mysql.MySqlConnector",
"table.include.list": "pawbridge_user.outbox_events",
"transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
"transforms.outbox.table.field.event.payload": "payload",
"transforms.outbox.route.topic.replacement": "user.favorite.events"
```

- **효과**: **DB 커밋 = 이벤트 발행 확정**. 애플리케이션은 Kafka를 전혀 모르고(발행 코드 0줄), Debezium이 binlog를 읽으므로 커넥터가 죽어도 재시작 시 offset부터 재발행 → **At-least-once 보장**.
- **`EventRouter` SMT 사용 이유**: Debezium 원본 메시지는 `{before, after, op, source}` 형태의 CDC 포맷인데, 컨슈머가 이걸 파싱하면 **컨슈머가 발행자의 테이블 스키마에 결합**됨. `EventRouter`가 `payload` 컬럼만 꺼내 순수 도메인 이벤트로 변환하고 `event_type`을 Kafka 헤더로 승격시켜 결합을 끊음.

#### SAGA 보상 트랜잭션

```java
// animal-service 처리 실패 시 → user.compensation.events 수신 → 찜 롤백
@KafkaListener(topics = "user.compensation.events")
public void consumeCompensationEvent(String message, Acknowledgment acknowledgment) { ... }
```

```java
// 멱등성 처리 — At-least-once의 필연적 중복에 대응
if (processedEventRepository.existsByEventId(eventId)) return;   // 1차 체크
try {
    processedEventRepository.save(ProcessedEvent.of(eventId, "ROLLBACK_FAVORITE_ADDED"));
} catch (Exception e) { return; }                                 // 2차: UNIQUE 제약으로 동시성 차단
favoriteRepository.deleteByUserUserIdAndAnimalId(userId, animalId);
```

- **핵심 설계**: `exists` 체크만으로는 두 컨슈머 스레드가 동시에 통과할 수 있음 → **비즈니스 로직 이전에 `ProcessedEvent`를 먼저 INSERT**하고 UNIQUE 제약 위반을 락으로 활용. DB 제약조건을 동시성 제어 수단으로 쓴 것.
- **수동 커밋 채택**: `enable-auto-commit: false` + `AckMode.MANUAL_IMMEDIATE`. 자동 커밋이면 처리 전에 offset이 커밋되어 장애 시 이벤트 유실. 처리 완료 후 `acknowledgment.acknowledge()`로 **At-least-once** 보장.
- **`Propagation.REQUIRES_NEW`** (`OutboxServiceImpl`): 부모 트랜잭션과 분리해 Outbox 저장을 독립 커밋.
- **`OutboxCleanupScheduler`**: Debezium이 읽고 지나간 outbox 레코드를 주기적으로 삭제해 테이블 무한 증식 방지.

> **면접 예상 질문**
> "왜 Outbox를 쓰나요?" → **Dual Write 문제**를 정확히 설명하고, "DB 트랜잭션과 메시지 발행은 원자적으로 묶을 수 없으므로, 발행 의도를 같은 트랜잭션 안의 테이블에 남기고 실제 발행은 CDC에 위임했다"
> "동물 서비스 컨슈머는 누가 만들었나요?" → 발행측(Outbox·Debezium·보상 컨슈머)이 내 담당이고, 소비측 `animal-service` 컨슈머는 팀원 담당으로 **이벤트 계약(토픽·payload 스키마)을 합의해 인터페이스로 분리**

---

### 2-4. 마이페이지 — 서비스 간 데이터 조합 (BFF 패턴)

`user-service`가 다른 서비스 데이터를 모아 하나의 응답으로 만드는 조합 계층.

| API | 조합 대상 |
|---|---|
| 내가 찜한 동물 | `animal-service` |
| 보호소 직원이 등록한 동물 | `animal-service` (careRegNo → shelterId → 동물 목록) |
| 내가 찜한 상품 / 주문 목록 / 장바구니 | `store-service` |

#### ① `OpenFeign` 선택

- **왜**: `RestTemplate`은 URL 문자열 조립과 응답 파싱을 매번 수동으로 해야 하는데, Feign은 **인터페이스 선언만으로** 클라이언트가 생성됨. 특히 Eureka와 결합해 `@FeignClient(name = "animal-service")`처럼 **서비스명으로 호출**하면 IP/포트를 몰라도 되고 클라이언트 사이드 로드밸런싱이 자동 적용.
- **효과**: 인스턴스 스케일아웃 시 코드 수정 0. 호출부가 로컬 메서드 호출처럼 읽힘.

#### ② N+1 회피 — 배치 조회 후 Map 조립 (`FavoriteServiceImpl.getFavorites`)

```java
// ❌ 찜 30개 → animal-service 호출 30번
// ✅ animalId 목록을 모아 1번 호출
List<Long> animalIds = favorites.stream().map(Favorite::getAnimalId).toList();
List<AnimalResponse> animals = animalServiceClient.getAnimalsByIds(animalIds);

// O(1) 조회용 Map으로 변환 후 조립 — 이중 루프(O(N*M)) 회피
Map<Long, AnimalResponse> animalMap = animals.stream()
        .collect(Collectors.toMap(AnimalResponse::getId, Function.identity()));
```

- **효과**: 네트워크 호출 **N회 → 1회**. 찜 30개 기준 응답 시간이 선형 증가에서 상수로 개선. 이를 위해 `animal-service`에 `POST /api/v1/mypage/animals/batch` 벌크 엔드포인트를 함께 설계.
- **`POST`를 쓴 이유**: ID 목록을 GET 쿼리스트링으로 넘기면 URL 길이 제한(약 2KB)에 걸림. 조회지만 body가 필요해 POST 채택.

#### ③ Graceful Degradation

```java
try {
    animals = animalServiceClient.getAnimalsByIds(animalIds);
} catch (Exception e) {
    log.error(...);
    // 실패 시에도 찜 목록은 반환 (동물 정보 없이)
}
// ...
return animal != null ? FavoriteWithAnimalDto.of(favorite, animal)
                      : FavoriteWithAnimalDto.ofWithoutAnimal(favorite);
```

- **왜**: `animal-service` 장애로 마이페이지 전체가 500이 되면 안 됨. 부가 정보 조회 실패를 전체 실패로 전파하지 않고 **부분 응답**으로 격하.
- **한계 인지(면접용)**: 코드에 `// Circuit Breaker 패턴` 주석이 있지만 실제 `Resilience4j` 적용은 미완이고 try-catch 폴백 수준. → "다음 단계로 `@CircuitBreaker` + fallback을 붙여 장애 시 빠른 실패와 자동 복구를 넣으려 했다"

#### ④ 권한 검증을 도메인 규칙으로

```java
if (user.getRole() != Role.ROLE_SHELTER) throw new UnauthorizedException("보호소 직원만 조회할 수 있습니다.");
```

- Gateway가 인증을, 서비스가 **도메인 규칙(보호소 소속 여부, careRegNo 보유)** 을 검증하는 2계층 구조. `careRegNo`를 JWT claim + `X-Care-Reg-No` 헤더로 전파해 같은 보호소 소속이 등록한 동물을 함께 조회.

---

### 2-5. 관리자 기능 — 3개 서비스에 걸친 구현

| 기능 | 서비스 |
|---|---|
| 회원 목록/상세/수정/삭제, 회원 검색(키워드+Role 필터) | `user-service` |
| 일별·기간별 가입자 통계, 전체 회원 수 | `user-service` |
| 게시글 목록/수정/삭제(작성자 무관), 오늘 등록 게시글 수 | `community-service` |
| 일별 동물 등록 통계 | `animal-service` |

#### ① 동적 검색 쿼리 — JPQL 조건부 파라미터

```java
@Query("SELECT u FROM User u WHERE " +
       "(:keyword IS NULL OR :keyword = '' OR " +
       " LOWER(u.email) LIKE LOWER(CONCAT('%', :keyword, '%')) OR ... ) AND " +
       "(:role IS NULL OR u.role = :role)")
Page<User> searchUsers(@Param("keyword") String keyword, @Param("role") Role role, Pageable pageable);
```

- **왜 이 방식**: QueryDSL을 쓰면 타입 안전하고 깔끔하지만 Q클래스 생성 설정과 의존성이 추가됨. 조건이 2개(keyword, role)뿐이라 `:param IS NULL OR ...` 패턴으로 **단일 쿼리에서 선택적 필터**를 처리하는 게 비용 대비 합리적이라 판단.
- **트레이드오프 인지(면접용)**: 조건이 늘어나면 쿼리가 급격히 복잡해지고 `IS NULL OR` 때문에 **인덱스를 못 타는 경우**가 생김. `LIKE '%keyword%'`도 선행 와일드카드라 인덱스 미사용 → "데이터가 커지면 Elasticsearch로 옮기거나 QueryDSL + 조건부 `BooleanBuilder`로 전환해야 한다". (이 프로젝트는 동물 검색에 Elasticsearch를 이미 쓰고 있어 자연스러운 연결)

#### ② DTO 프로젝션으로 통계 쿼리

```java
@Query("SELECT new com.pawbridge.userservice.dto.response.DailySignupStatsResponse(" +
       "CAST(u.createdAt AS LocalDate), COUNT(u)) FROM User u " +
       "WHERE CAST(u.createdAt AS LocalDate) BETWEEN :startDate AND :endDate " +
       "GROUP BY CAST(u.createdAt AS LocalDate)")
```

- **왜**: 엔티티 전체를 메모리로 올려 Java에서 `groupingBy`로 집계하면 회원 10만 명일 때 10만 건이 힙에 올라옴. **DB에서 GROUP BY로 집계하고 결과 행(날짜 수만큼)만 DTO로 받음**.
- **효과**: 30일 조회 시 반환 행이 최대 30건. 메모리 사용량이 데이터 규모와 무관해짐.

#### ③ 데이터 없는 날짜 채우기 — DB가 못 하는 일은 애플리케이션에서

```java
private List<DailySignupStatsResponse> fillMissingDates(LocalDate start, LocalDate end, List<...> dbResults) {
    Map<LocalDate, Long> dateCountMap = dbResults.stream().collect(toMap(...::date, ...::count));
    while (!currentDate.isAfter(end)) {
        result.add(new DailySignupStatsResponse(currentDate, dateCountMap.getOrDefault(currentDate, 0L)));
        currentDate = currentDate.plusDays(1);
    }
}
```

- **왜**: `GROUP BY`는 **가입자가 0명인 날은 행 자체를 반환하지 않음**. 프론트가 차트를 그릴 때 날짜가 끊겨 그래프가 왜곡됨. DB에서 해결하려면 날짜 테이블/재귀 CTE가 필요해 복잡도가 큼 → 애플리케이션에서 연속 날짜를 생성하고 Map으로 O(1) 조회해 채움.
- **효과**: 프론트는 항상 `startDate ~ endDate` 길이의 연속 배열을 받아 별도 가공 없이 차트 렌더링.

#### ④ 기간별 통계를 단일 API로 (`getSignupPeriods`)

오늘 / 최근 7일 / 최근 30일 / 이번 달을 **4개 쿼리 1개 응답**으로 묶음. 대시보드에서 4번 호출하던 것을 1번으로 줄여 왕복 지연 감소.

#### ⑤ Gateway RewritePath로 프론트 URL 규약 분리

```yaml
- id: admin-users
  predicates:
    - Path=/api/admin/users, /api/admin/users/**, /api/admin/stats/daily-signups
  filters:
    - RewritePath=/api/admin/(?<segment>.*), /api/v1/admin/${segment}
    - JwtAuthorization
```

- **왜**: 프론트는 `/api/admin/**`이라는 단일 관리자 네임스페이스를 원했지만, 실제로는 `user-service`·`community-service`·`animal-service` 3곳에 흩어져 있음. Gateway에서 라우팅+경로 재작성으로 **프론트에게는 하나의 관리자 API처럼 보이게** 함.
- **주의점(직접 처리한 이슈)**: Rewrite **후** 경로로 권한을 체크해야 하므로 필터에서 `/api/v1/admin/**` 패턴을 매칭 — 순서를 잘못 잡으면 인가가 우회됨.

```java
// RewritePath 필터 후 변환된 경로를 체크
if (pathMatcher.match("/api/v1/admin/**", path)) return true;   // 모든 메서드에 ADMIN 필요
```

---

### 2-6. 전사 공통 규약 정립 (팀 전체에 전파)

`user-service`에서 만든 예외/응답 규약을 `community-service`에도 직접 이식 (커밋 `7839e0b`).

```java
ResponseDTO<T>              // { code, message, data } 통일 응답 래퍼
ErrorCode (enum)            // HttpStatus + 메시지를 한 곳에서 관리
ApplicationException        // ErrorCode를 들고 있는 커스텀 예외 최상위
GlobalExceptionRestAdvice   // @RestControllerAdvice, 12종 예외 핸들링
```

- **왜**: 서비스마다 응답 구조가 다르면 프론트가 서비스별 분기 코드를 짜야 하고, 예외를 컨트롤러에서 try-catch하면 비즈니스 로직에 에러 처리가 섞임.
- **커버 범위**: `MethodArgumentNotValidException`(검증 실패 → 필드별 메시지 조합), `HttpMessageNotReadableException`(JSON 파싱), `HttpRequestMethodNotSupportedException`, `MissingServletRequestParameterException`, `TypeMismatchException`, `NoHandlerFoundException`, `DataAccessException`, `RedisConnectionFailureException` 등.
- **효과**: 서비스 코드는 `throw new UserNotFoundException()` 한 줄. HTTP 상태 코드·메시지 매핑은 `ErrorCode`에서 일괄 관리. `user-service`에만 **도메인 예외 40여 개**를 정의해 에러 원인이 타입으로 드러나게 함.
- **Gateway까지 일관성 확장**: WebFlux인 Gateway는 `@RestControllerAdvice`를 못 쓰므로 `ErrorResponse.of()` + `ObjectMapper`로 직접 직렬화해 **다른 서비스와 동일한 JSON 구조**를 반환하도록 구현. (커밋 `86c6dfe`)

---

### 2-7. 커뮤니티 서비스 — 초기 구현 및 개선

- **초기 구현** (커밋 `d848c41`, `438fd4d`): 게시글/댓글 CRUD
- **S3 영상 업로드 지원** (커밋 `7839e0b`): `Content-Type` 화이트리스트 검증(이미지 5종 + 영상 4종), **`posts/images/` · `posts/videos/` 경로 분리 저장**
  - *왜 확장자가 아니라 Content-Type인가*: 파일명 확장자는 위조가 쉬움. MIME 타입 검증이 한 단계 더 안전 (완벽하진 않아 magic byte 검증이 다음 단계)
- **부분 수정 지원** (커밋 `d342803`): 제목/내용/파일을 **개별 필드로 받아 null이 아닌 것만 업데이트**. 기존엔 전체를 보내야 해서 제목만 바꾸려도 본문 전체를 재전송해야 했던 문제 해결
- **작성자 정보 개선** (커밋 `2acb874`): 게시글 응답에 `authorId`뿐 아니라 `nickname`도 포함 → 프론트가 사용자 정보를 별도 조회하는 추가 요청 제거

---

## 3. 사용 기술 스택 정리

| 분류 | 기술 | 선택 이유 (한 줄) |
|---|---|---|
| 언어/프레임워크 | Java 17, Spring Boot 3.4.11 | Record·Sealed 등 최신 문법, Jakarta 네임스페이스 |
| 인증 | Spring Security, JJWT 0.12.5, BCrypt | 표준 인증 파이프라인 재사용, 단방향 해시 + salt |
| 소셜 로그인 | Spring OAuth2 Client (Google) | Authorization Code 흐름을 프레임워크가 처리 |
| MSA | Spring Cloud Gateway, Eureka, Config Server, OpenFeign | 중앙 인가 / 동적 서비스 탐색 / 설정 외부화 / 선언적 HTTP |
| 캐시·상태 | Redis (+ Lua Script) | TTL 기반 휘발성 데이터, 원자적 rate limiting |
| 메시징 | Kafka, Debezium (MySQL CDC) | Outbox 패턴으로 Dual Write 해결, 서비스 간 비동기 결합 해제 |
| 영속성 | Spring Data JPA, MySQL, JPA Auditing | 낙관적 규약 + `@CreatedDate` 자동화 |
| 저장소 | AWS S3 | 이미지/영상 정적 파일 오프로딩 |
| 관측 | Micrometer + Prometheus, Zipkin(Brave) | MSA 분산 추적, 메트릭 수집 |

---

## 4. 면접 예상 질문 & 답변 시나리오

### Q1. JWT를 왜 localStorage에서 쿠키로 바꿨나요?

> localStorage는 JavaScript로 접근 가능해서 XSS가 한 번 성공하면 토큰이 그대로 유출됩니다. 저희는 사용자 입력이 렌더링되는 커뮤니티 기능이 있어 이 리스크가 실재했습니다. `HttpOnly` 쿠키로 바꾸면 JS 접근이 차단됩니다. 다만 쿠키는 자동 전송되므로 CSRF에 노출되는데, 프론트가 별도 도메인이라 `SameSite=Strict`를 쓸 수 없어서 `SameSite=None + Secure` 조합에 CORS `allowedOriginPatterns`로 출처를 명시적으로 제한했습니다. 개발 환경(localhost)은 `Secure`·`Domain`이 오히려 방해가 되어 `spring.profiles.active` 기반으로 쿠키 속성을 분기했습니다.

### Q2. Access Token은 서버에 저장 안 하고 Refresh Token은 DB에 저장한 이유는?

> Access Token까지 매 요청 DB 조회하면 JWT의 Stateless 이점이 사라집니다. 실제로 Gateway는 서명 검증만 하고 DB를 안 봅니다. 반대로 Refresh Token은 탈취 시 강제 무효화가 필요한데 Stateless로는 불가능하므로 DB에 저장하고, 로그아웃 때 `deleteByUserId()`로 즉시 무효화합니다. 재발급 시 Refresh Token도 함께 교체(Rotation)해 같은 토큰 재사용을 차단했습니다.

### Q3. Outbox 패턴을 왜 도입했나요?

> `favoriteRepository.save()` 후 `kafkaTemplate.send()`를 호출하면 DB는 커밋됐는데 Kafka 발행이 실패할 수 있습니다(Dual Write 문제). 그러면 찜은 등록됐지만 동물 서비스의 카운트는 영원히 안 오릅니다. DB 트랜잭션과 메시지 발행은 원자적으로 묶을 수 없으니, **발행 의도를 같은 트랜잭션 안의 `outbox_events` 테이블에 저장**하고 실제 발행은 Debezium이 binlog를 읽어 처리하게 했습니다. DB 커밋이 곧 발행 확정이 되고, 커넥터가 죽어도 offset부터 재발행되니 유실이 없습니다.

### Q4. Kafka는 At-least-once인데 중복 처리는 어떻게 막았나요?

> `processed_events` 테이블에 `event_id`를 UNIQUE로 두고 멱등성을 확보했습니다. 중요한 건 순서인데, `exists` 체크만 하면 두 컨슈머 스레드가 동시에 통과할 수 있어서 **비즈니스 로직 전에 `ProcessedEvent`를 먼저 INSERT**하고 UNIQUE 제약 위반을 락처럼 활용했습니다. DB 제약조건을 동시성 제어 수단으로 쓴 셈입니다. 그리고 `enable-auto-commit=false` + `MANUAL_IMMEDIATE`로 처리 완료 후에만 offset을 커밋합니다.

### Q5. Redis에서 Lua 스크립트를 쓴 이유는?

> 이메일 발송 횟수 제한을 `GET → 비교 → INCR`로 구현하면 동시 요청이 전부 한도 미달을 읽고 통과합니다. Redis는 싱글 스레드로 스크립트를 원자적으로 실행하므로 Lua로 묶으면 중간 개입이 불가능합니다. 분산 락도 가능하지만 단일 키 연산에 락 획득 비용과 데드락 리스크를 감수할 이유가 없었습니다. 추가로 `INCR` 결과가 1일 때만 `EXPIRE`를 걸어야 하는데, 매번 걸면 요청이 계속 오는 동안 TTL이 갱신돼 제한 창이 리셋되기 때문입니다.

### Q6. MSA인데 email-service를 왜 합쳤나요? (역방향 리팩토링)

> 이메일 인증은 회원가입의 동기적 선행 조건이고, 두 서비스가 같은 Redis 키를 공유하고 있었습니다. 사실상 결합돼 있는데 배포·네트워크 홉만 늘리는 구조였습니다. **함께 변하고 데이터를 공유하는 것은 같은 서비스에 둬야 한다**고 판단해 `user-service/email/` 패키지로 흡수했습니다. Feign 클라이언트와 별도 Gradle 모듈을 제거하며 1,300줄가량 삭제했습니다. MSA는 목적이 아니라 수단이라고 생각합니다.

### Q7. 인가를 Gateway에 집중시킨 이유와 그 위험은?

> 서비스가 6개인데 각자 JWT를 파싱하면 `jwt.secret`이 전부에 퍼지고, 정책 하나 바꿀 때 6개를 동시 배포해야 합니다. Gateway에서 검증하고 `X-User-Id`, `X-User-Role`, `X-Care-Reg-No` 헤더로 신원을 전파하니 하위 서비스는 `@RequestHeader`만 읽으면 됩니다. **위험은 Gateway를 우회한 직접 요청**입니다. 현재는 서비스가 내부망에만 노출된다는 전제이고, 프로덕션이라면 서비스 간 mTLS나 내부망 전용 검증 필터를 추가해야 합니다. 실제로 `SecurityConfig`에 그 고민을 주석으로 남겨뒀습니다.

### Q8. 마이페이지 성능은 어떻게 개선했나요?

> 찜 목록에서 각 동물 정보를 개별 조회하면 찜 30개에 Feign 호출 30번, N+1이 네트워크로 확장된 형태였습니다. `animalId`를 모아 `POST /mypage/animals/batch`로 한 번에 조회하고, 결과를 `Map<Long, AnimalResponse>`로 변환해 O(1)로 조립했습니다. 호출 N회가 1회로 줄었고, 이중 루프 O(N*M)도 회피했습니다. ID 목록이 길어 URL 길이 제한에 걸릴 수 있어 조회지만 POST로 설계했습니다.

### Q9. 개선하고 싶은 부분은? (반드시 준비할 질문)

> 세 가지입니다.
> ① **Circuit Breaker 미완성** — Feign 호출 실패를 try-catch 폴백으로만 막고 있어 장애 시 타임아웃까지 대기합니다. Resilience4j로 빠른 실패와 자동 복구를 붙이고 싶습니다.
> ② **테스트 부재** — `UserServiceApplicationTests` 하나뿐입니다. 특히 Outbox·멱등성 로직은 동시성 테스트가 필요합니다.
> ③ **관리자 검색 쿼리** — `LIKE '%keyword%'`가 인덱스를 못 타므로 데이터가 커지면 QueryDSL 전환이나 Elasticsearch 활용이 필요합니다.

---

## 5. 한 줄 요약 (이력서용)

> **MSA 기반 유기동물 플랫폼의 인증·인가 도메인 및 API Gateway 오너.** Spring Security + JWT 인증 체계와 Google OAuth2 소셜 로그인을 구현하고, 토큰 전달 방식을 localStorage에서 HttpOnly 쿠키로 전환해 XSS 토큰 탈취 경로를 차단. Gateway에 Role 기반 중앙 인가 필터를 구현해 6개 서비스의 JWT 검증 로직 중복을 제거. **Transactional Outbox + Debezium CDC + Kafka**로 분산 트랜잭션의 Dual Write 문제를 해결하고 SAGA 보상 트랜잭션 및 멱등성 처리를 구현. Redis Lua 스크립트로 원자적 rate limiting을 적용해 이메일 인증 남용을 차단. 전사 공통 응답·예외 규약을 정립해 다른 서비스에 전파. **79 커밋 / +11,893줄**

---

## 6. 우선순위 가이드

준비 시간이 부족하다면 **Q3(Outbox) → Q4(멱등성) → Q1(쿠키)** 순으로 집중.
이 세 개가 가장 차별화되는 답변이고, 나머지(JWT 기본, Feign, 계층 구조)는 흔한 주제라 짧게 정리해도 충분합니다.

주요 참조 파일:

- `api-gateway/src/main/java/com/pawbridge/apigateway/filter/JwtAuthorizationGatewayFilterFactory.java`
- `user-service/src/main/java/com/pawbridge/userservice/util/CookieUtil.java`
- `user-service/src/main/java/com/pawbridge/userservice/service/FavoriteServiceImpl.java`
- `user-service/src/main/java/com/pawbridge/userservice/handler/CompensationEventHandler.java`
- `user-service/src/main/java/com/pawbridge/userservice/email/config/RedisConfig.java`
- `infrastructure/kafka/connectors/user-outbox-connector.json`
