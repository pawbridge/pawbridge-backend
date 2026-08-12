# 02. api-gateway — 모든 요청이 지나가는 단 하나의 문

> 자바 파일 6개밖에 없지만, 이 프로젝트에서 **가장 중요하고 가장 위험한** 서비스입니다.
> 6개 서비스의 인증·인가를 여기서 혼자 담당하기 때문입니다.

파일 목록:

```
api-gateway/src/main/java/com/pawbridge/apigateway/
├── ApiGatewayApplication.java
├── config/SecurityConfig.java
├── filter/JwtAuthorizationGatewayFilterFactory.java   ← 핵심 (약 250줄)
└── util/
    ├── JwtUtil.java
    └── ErrorResponse.java
```

---

## 1. 게이트웨이가 왜 필요한가

게이트웨이가 없다면 프론트엔드는 이렇게 호출해야 합니다:

```
회원 정보    → http://10.0.4.11:8081/api/v1/users/me
동물 목록    → http://10.0.3.22:9020/api/v1/animals
상품 목록    → http://10.0.4.11:8085/api/v1/products
```

문제가 줄줄이 생깁니다.

- 프론트가 **7개 주소를 다 알아야** 합니다. 서버가 바뀌면 프론트도 배포해야 합니다.
- 브라우저 입장에서 도메인이 7개니 **CORS 설정을 7곳에** 해야 합니다.
- **JWT 검증 코드를 7곳에** 복붙해야 합니다. 한 곳에서 실수하면 그 서비스만 뚫립니다.
- 쿠키는 도메인 단위라 **로그인 쿠키를 공유하기가 까다롭습니다.**

게이트웨이를 두면 프론트는 `https://api.pawbridge.kr` **한 곳만** 알면 됩니다.
CORS도, JWT 검증도, 라우팅도 한 곳에서 끝납니다.

```
                                       ┌→ user-service
브라우저 → nginx(443) → api-gateway ───┼→ animal-service
                          ↑            ├→ community-service
                    JWT 검증 + 인가     ├→ store-service
                    X-User-* 헤더 주입   └→ payment-service
```

---

## 2. Spring Cloud Gateway의 3요소: Predicate / Filter / URI

라우팅 설정은 전부 이 세 가지 조합입니다.

```yaml
- id: store-service-orders          # 이름 (로그·디버깅용)
  uri: lb://store-service           # ③ 어디로 보낼지
  predicates:                       # ① 어떤 요청을 잡을지 (조건)
    - Path=/api/orders/**
  filters:                          # ② 보내기 전/후에 무엇을 할지
    - JwtAuthorization
```

- **Predicate(조건)** — "이 요청이 이 라우트에 해당하나?" 를 판단. `Path=`, `Method=`, `Header=` 등.
- **Filter(필터)** — 요청/응답을 **가공**. 헤더 추가, 경로 변경, 인증 검사 등.
- **URI(목적지)** — `lb://` 를 쓰면 Eureka에서 이름으로 찾아 로드밸런싱합니다.

### ⚠️ 라우트는 "위에서 아래로" 첫 번째 일치하는 것이 이깁니다

```yaml
# ↓ 순서 중요! (실제 설정의 주석에도 "순서 중요"라고 적혀 있음)
- id: store-service-products-public      # GET  /api/products/**  → 필터 없음(공개)
  predicates: [Path=/api/products/**, Method=GET]

- id: store-service-products-protected   # POST /api/products/** → JWT 필요
  predicates: [Path=/api/products/**, Method=POST,PUT,PATCH,DELETE]
```

만약 순서를 바꿔 `Method` 조건 없는 라우트를 위에 두면, **GET도 POST도 전부 그 라우트가 먹어버려서**
아래 라우트는 영원히 실행되지 않습니다. 그러면 상품 등록 API가 인증 없이 열리거나(위험) 상품 조회가
로그인 필수가 됩니다(불편).

같은 이유로 `user-service-email`, `user-service-oauth2` 라우트가 `user-service`보다 **위에** 있습니다.
회원가입 전(=토큰 없음)에 호출해야 하는 경로이므로 JWT 필터가 붙은 라우트에 먹히면 안 됩니다.

---

## 3. 핵심 파일 정독 — JwtAuthorizationGatewayFilterFactory

[filter/JwtAuthorizationGatewayFilterFactory.java](../api-gateway/src/main/java/com/pawbridge/apigateway/filter/JwtAuthorizationGatewayFilterFactory.java)

### 3-1. 클래스 이름이 왜 이렇게 긴가 — 네이밍 규칙이다

```java
public class JwtAuthorizationGatewayFilterFactory
        extends AbstractGatewayFilterFactory<JwtAuthorizationGatewayFilterFactory.Config> {
```

Spring Cloud Gateway는 **클래스 이름에서 `GatewayFilterFactory`를 뗀 나머지**를 필터 이름으로 씁니다.

```
JwtAuthorization + GatewayFilterFactory
└──────┬───────┘
       └→ yml에서 `- JwtAuthorization` 으로 쓸 수 있게 된다
```

즉 이름을 `MyJwtFilter`로 바꾸면 yml의 `- JwtAuthorization`이 **"그런 필터 없음" 에러**가 납니다.
이름이 곧 설정 키인 구조입니다.

`Config` 클래스는 필터에 파라미터를 넘기기 위한 것입니다(`- JwtAuthorization=값1,값2` 형태).
이 프로젝트는 파라미터를 쓰지 않으므로 빈 클래스입니다.

### 3-2. 반환 타입이 `Mono<Void>` 인 이유 — 게이트웨이는 WebFlux다

```java
public GatewayFilter apply(Config config) {
    return (exchange, chain) -> {
        ...
        return chain.filter(exchange);   // ← Mono<Void>를 반환
    };
}
```

여기서 초보자가 가장 많이 혼란스러워하는 부분입니다.

| | 일반 서비스 (user-service 등) | api-gateway |
|---|---|---|
| 웹 스택 | Spring MVC (Servlet, 동기) | **Spring WebFlux (Netty, 비동기)** |
| 요청 객체 | `HttpServletRequest` | `ServerHttpRequest` |
| 반환 | `return responseDto;` | `return Mono<Void>` |
| 스레드 모델 | 요청 1개 = 스레드 1개 | 적은 스레드로 수천 요청 처리 |

**왜 게이트웨이만 WebFlux인가?** 게이트웨이는 하는 일이 "받아서 넘기고 기다리기"뿐입니다.
계산은 거의 없고 **대기 시간이 대부분**입니다. 요청마다 스레드를 하나씩 붙이면 스레드가 놀면서 메모리만
잡아먹습니다. 비동기(이벤트 루프)로 만들면 적은 스레드로 훨씬 많은 요청을 중계할 수 있습니다.

`Mono<Void>`는 "언젠가 끝날 작업(결과값은 없음)"이라는 약속입니다. 값을 바로 돌려주는 게 아니라
**"이 작업을 이어서 하라"는 계획서를 반환**한다고 이해하면 됩니다.

그래서 `chain.filter(exchange)`는 "다음 필터로 넘겨라"이고, 이걸 **`return` 해야** 실제로 실행됩니다.
반환하지 않으면 요청이 그 자리에서 멈춥니다. 서블릿 필터의 `chain.doFilter()`와 비슷하지만,
호출이 아니라 **반환**이라는 점이 다릅니다.

### 3-3. 필터의 처리 흐름 — 5단계

```java
return (exchange, chain) -> {
    String path = exchange.getRequest().getURI().getPath();

    // ① 화이트리스트면 검사 없이 통과
    if (isWhitelisted(path)) return chain.filter(exchange);

    // ② 쿠키에서 accessToken 꺼내기 (없으면 401)
    String token = extractTokenFromCookie(request);
    if (token == null) return onError(exchange, "인증 토큰이 필요합니다.", UNAUTHORIZED);

    // ③ 서명·만료 검증 (틀리면 401)
    if (!jwtUtil.validateAccessToken(token)) return onError(..., UNAUTHORIZED);

    // ④ Role 기반 인가 (권한 부족이면 403)
    if (isAdminOnlyPath(method, path) && !role.equals("ROLE_ADMIN")) return onError(..., FORBIDDEN);
    if (isNonUserPath(method, path) && role.equals("ROLE_USER"))     return onError(..., FORBIDDEN);

    // ⑤ 신분증을 헤더로 바꿔 달아서 뒤로 넘김
    var requestBuilder = request.mutate()
            .header("X-User-Id", userId.toString())
            .header("X-User-Email", email)
            .header("X-User-Name", name)
            .header("X-User-Role", role);
    return chain.filter(exchange.mutate().request(requestBuilder.build()).build());
};
```

**401과 403의 구분이 정확합니다** — 이건 잘 된 부분입니다.

- **401 Unauthorized** = "네가 누군지 모르겠다" (토큰 없음/만료/위조)
- **403 Forbidden** = "네가 누군지는 알지만 권한이 없다" (ROLE_USER가 관리자 API 호출)

프론트엔드는 이 구분으로 동작을 나눕니다. 401이면 **토큰 재발급을 시도**하고, 403이면 **재발급해도
소용없으니 "권한 없음" 화면**을 띄웁니다. 둘을 뭉개면 403인데 무한 재발급 루프에 빠집니다.

### 3-4. ⑤번이 이 아키텍처의 핵심 — "JWT를 헤더로 번역한다"

```java
.header("X-User-Id", userId.toString())
```

게이트웨이 뒤의 서비스들은 **JWT를 아예 모릅니다.** 컨트롤러가 이렇게 생겼습니다:

```java
// community-service/controller/PostController.java
@PostMapping
public ResponseEntity<?> createPost(@RequestBody PostRequest req,
                                    @RequestHeader("X-User-Id") Long userId) { ... }
```

**얻는 것**: JWT 라이브러리, 시크릿 키, 파싱 코드, 만료 처리가 게이트웨이에만 있습니다.
6개 서비스에서 그 코드가 전부 사라졌습니다. 시크릿을 교체할 때 고칠 곳도 한 곳입니다.

**잃는 것**: 서비스들은 **"이 헤더는 게이트웨이가 넣은 것"이라고 무조건 믿습니다.**
게이트웨이를 거치지 않고 서비스에 직접 요청하면, 헤더를 손으로 써서 아무나 될 수 있습니다.
→ 이 위험은 [6-1](#6-1-게이트웨이-우회--헤더를-무조건-믿는다)에서 자세히 다룹니다.

**`mutate()`에 대해 알아둘 점**: `ServerHttpRequest`는 불변(immutable) 객체입니다. 헤더를 바꿀 수 없습니다.
그래서 `mutate()`로 "변경된 복사본"을 만들어 교체합니다. 그리고 `.header(name, value)`는 **추가가 아니라
덮어쓰기(set)** 입니다. 클라이언트가 `X-User-Id: 999`를 보내도 게이트웨이 값으로 교체되므로,
**게이트웨이를 통과하는 경로에서는 헤더 위조가 통하지 않습니다.**

### 3-5. `AntPathMatcher` — 와일드카드 경로 매칭

```java
private final AntPathMatcher pathMatcher = new AntPathMatcher();

private boolean isWhitelisted(String path) {
    return WHITELIST.stream().anyMatch(pattern -> pathMatcher.match(pattern, path));
}
```

`equals`로 비교하면 `/api/v1/animals/5` 같은 동적 경로를 처리할 수 없습니다.
Ant 스타일 패턴은 세 가지 기호를 씁니다:

| 기호 | 의미 | 예 |
|---|---|---|
| `?` | 글자 1개 | `/api/v?` → `/api/v1` ✓ |
| `*` | **한 세그먼트** 안의 임의 문자 | `/api/v1/animals/*` → `/api/v1/animals/5` ✓ , `/api/v1/animals/5/photos` ✗ |
| `**` | **여러 세그먼트** | `/api/v1/admin/**` → `/api/v1/admin/users/3/roles` ✓ |

화이트리스트에 `/api/v1/animals/*`(별 하나)를 쓴 것은 의도적입니다. 상세 조회는 열지만
하위 경로까지 통째로 열지는 않겠다는 뜻입니다.

### 3-6. 인가 규칙을 자바 상수 배열로 관리한다

```java
private static final List<String> ADMIN_ONLY_PATHS = List.of(
        "POST:/api/v1/shelters",
        "DELETE:/api/v1/shelters/*",
        "POST:/api/products",
        ...
);

private boolean matchesMethodAndPath(String pattern, String method, String path) {
    String[] parts = pattern.split(":", 2);        // "POST:/api/products" → ["POST", "/api/products"]
    return parts[0].equals(method) && pathMatcher.match(parts[1], path);
}
```

`"메서드:경로"` 를 한 문자열에 담고 `split(":", 2)`로 쪼개는 방식입니다.
`split`의 두 번째 인자 `2`가 중요합니다 — 경로에 `:`가 또 나와도 첫 번째에서만 자릅니다.

**직관적이고 코드도 짧습니다. 다만 이 방식의 대가가 큽니다** → [6-2](#6-2-인가-목록과-경로-리라이트가-엇갈려서-생긴-실제-구멍) 참고.

### 3-7. 관리자 경로만 별도 처리

```java
private boolean isAdminOnlyPath(String method, String path) {
    // /api/v1/admin/** 은 메서드 무관하게 전부 ADMIN
    if (pathMatcher.match("/api/v1/admin/**", path)) {
        return true;
    }
    return ADMIN_ONLY_PATHS.stream().anyMatch(...);
}
```

**이 방식이 훨씬 안전합니다.** 목록에 일일이 등록하는 대신 **"경로 접두사 = 권한"** 이라는 규칙 하나로
처리합니다. 새 관리자 API를 만들 때 `/api/v1/admin/` 아래에 두면 **자동으로 보호됩니다.**
목록에 추가하는 걸 잊어서 뚫리는 일이 없습니다.

실제로 이 프로젝트에서 **믿을 수 있게 동작하는 인가 규칙은 이것뿐입니다.**
관리자 라우트들은 모두 `RewritePath`로 `/api/v1/admin/...` 형태로 바뀐 뒤 필터를 타기 때문입니다.

### 3-8. 에러 응답을 다른 서비스와 똑같은 모양으로 맞췄다

```java
private Mono<Void> onError(ServerWebExchange exchange, String message, HttpStatus status) {
    response.setStatusCode(status);
    response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
    ErrorResponse errorResponse = ErrorResponse.of(status.value(), message);
    byte[] bytes = objectMapper.writeValueAsBytes(errorResponse);
    DataBuffer buffer = response.bufferFactory().wrap(bytes);
    return response.writeWith(Mono.just(buffer));
}
```

[util/ErrorResponse.java](../api-gateway/src/main/java/com/pawbridge/apigateway/util/ErrorResponse.java)의
주석에 의도가 적혀 있습니다: *"다른 서비스의 ResponseDTO와 동일한 구조"*.

**왜 중요한가**: 게이트웨이가 막았을 때와 서비스가 막았을 때 응답 모양이 다르면, 프론트엔드가
에러 처리를 두 갈래로 써야 합니다.

```json
// 게이트웨이의 ErrorResponse         // 서비스의 ResponseDTO
{ "code": 401,                        { "code": 400,
  "message": "...",                     "message": "...",
  "data": null }                        "data": null }
```

같은 모양이므로 프론트는 한 가지 처리기로 끝냅니다. **작지만 팀 전체의 작업량을 줄이는 결정입니다.**

`DataBuffer`가 등장하는 이유는 WebFlux라서입니다. `response.getWriter().write()` 같은 서블릿 API가
없으므로 바이트 버퍼를 직접 만들어 `writeWith`로 흘려보냅니다.

### 3-9. JwtUtil — 검증만 있고 생성이 없다

[util/JwtUtil.java](../api-gateway/src/main/java/com/pawbridge/apigateway/util/JwtUtil.java)

```java
public JwtUtil(@Value("${jwt.secret}") String secret) {
    this.secretKey = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
}

private Claims getClaims(String token) {
    return Jwts.parser()
            .verifyWith(secretKey)          // 서명 검증
            .build()
            .parseSignedClaims(token)       // 만료됐으면 여기서 예외
            .getPayload();
}
```

게이트웨이의 `JwtUtil`에는 **`createToken`이 없습니다.** 발급은 user-service의
[JwtProvider](../user-service/src/main/java/com/pawbridge/userservice/jwt/JwtProvider.java)만 합니다.
역할이 깔끔하게 나뉘어 있습니다: **user-service = 발급, gateway = 검증.**

**HMAC 대칭키의 의미**: 발급과 검증에 **같은 키**를 씁니다. 그래서 `jwt.secret`이 두 서비스에 모두
들어가 있습니다(config repo에서 같은 `{cipher}` 값). 대칭키의 단점은 **검증만 하는 쪽도 발급 능력을 갖게
된다**는 점입니다. 서비스가 늘어나면 비대칭키(RS256: 발급=비밀키, 검증=공개키)로 바꾸는 게 안전합니다.

**`validateAccessToken`이 `try-catch`로 `false`를 반환하는 이유**:

```java
public boolean validateAccessToken(String token) {
    try { getClaims(token); return true; }
    catch (Exception e) { return false; }
}
```

JJWT는 만료·서명오류·형식오류를 모두 **예외**로 던집니다. 필터에서는 "유효한가?"라는 boolean만 필요하니
예외를 삼켜 단순화했습니다. 다만 이 때문에 **"만료됨"과 "위조됨"을 구분할 수 없습니다.**
만료라면 프론트에 "재발급하라"고 알려줄 수 있는데, 지금은 둘 다 똑같은 401입니다.
`ExpiredJwtException`만 따로 잡아 다른 메시지를 주면 프론트가 더 정확히 대응할 수 있습니다.

---

## 4. SecurityConfig — 왜 전부 permitAll인가

[config/SecurityConfig.java](../api-gateway/src/main/java/com/pawbridge/apigateway/config/SecurityConfig.java)

```java
http.csrf(ServerHttpSecurity.CsrfSpec::disable)
    .cors(ServerHttpSecurity.CorsSpec::disable)
    .authorizeExchange(exchange -> exchange
            .pathMatchers(HttpMethod.OPTIONS, "/**").permitAll()
            .anyExchange().permitAll()      // ← 전부 허용
    );
```

"보안 설정인데 전부 허용?" 하고 놀랄 수 있지만 의도된 것입니다.

**인증은 Spring Security가 아니라 `JwtAuthorization` 게이트웨이 필터가 합니다.**
여기서 Security가 또 막으면 이중 관문이 되어 규칙이 두 곳에 흩어집니다.
그래서 Security는 통로만 열어두고 판단은 게이트웨이 필터에 위임합니다.

세 가지 세부 결정:

- **`csrf.disable()`** — CSRF 토큰은 세션 기반 인증을 위한 방어입니다. 여기는 stateless(JWT)이므로
  서버가 세션을 갖지 않습니다. 단, **쿠키로 토큰을 나르므로 CSRF 위험 자체는 남습니다.**
  이 프로젝트는 그 방어를 쿠키의 `SameSite` 속성에 맡기고 있습니다
  ([03-user-service.md](03-user-service.md#5-쿠키-전략--localstorage에서-옮겨온-이유) 참고).
- **`cors.disable()`** — CORS를 안 쓴다는 뜻이 아니라, **Security 레벨에서 하지 않는다**는 뜻입니다.
  `application.yml`의 `spring.cloud.gateway.globalcors`가 담당합니다. 두 곳에서 CORS를 설정하면
  헤더가 중복되어(`Access-Control-Allow-Origin`이 두 번) 브라우저가 오히려 요청을 거부합니다.
- **`OPTIONS` 명시 허용** — 브라우저는 실제 요청 전에 `OPTIONS`로 "이 요청 보내도 되나?"를 물어봅니다
  (preflight). 이 요청에는 **쿠키도, 토큰도 없습니다.** 막으면 모든 CORS 요청이 실패합니다.

---

## 5. CORS 설정 — `allowCredentials: true`의 함정

```yaml
globalcors:
  cors-configurations:
    '[/**]':
      allowedOriginPatterns:            # ← Patterns (뒤에 s, 그리고 Origins가 아님)
        - "https://pawbridge.kr"
        - "https://*.pawbridge.kr"
      allowCredentials: true            # 쿠키 전송 허용
```

**`allowedOrigins`가 아니라 `allowedOriginPatterns`인 이유**가 핵심입니다.

CORS 명세에는 안전장치가 있습니다: **`allowCredentials: true`(쿠키 허용)와 `allowedOrigins: "*"`
(모든 출처)를 동시에 쓸 수 없습니다.** 허용하면 아무 사이트나 사용자의 로그인 쿠키를 실어 요청을 보낼 수
있게 되기 때문입니다. 실제로 그렇게 설정하면 Spring이 **시작 시점에 예외를 던지고 죽습니다.**

그런데 `*.pawbridge.kr`처럼 **와일드카드가 섞인 패턴**은 쓰고 싶습니다. 그래서 Spring이 만든 것이
`allowedOriginPatterns`입니다. 요청이 올 때 패턴과 대조해서 **실제 Origin 값을 그대로 응답 헤더에
반환**합니다(`*`를 반환하지 않음). 명세를 지키면서 와일드카드를 쓰는 방법입니다.

---

## 6. ⚠️ 실제로 확인된 두 가지 위험

여기부터는 "개선하면 좋다"가 아니라 **지금 뚫려 있는 것**입니다.

### 6-1. 게이트웨이 우회 — 헤더를 무조건 믿는다

[deployment/docker-compose-node-4.yml](../deployment/docker-compose-node-4.yml)이 서비스 포트를
호스트에 그대로 노출합니다:

```yaml
user-service:      ports: ["8081:8081"]
store-service:     ports: ["8085:8085"]
community-service: ports: ["8089:8089"]
payment-service:   ports: ["8084:8084"]
```

그리고 이 서비스들은 JWT를 검증하지 않고 `X-User-Id` 헤더만 믿습니다. 따라서:

```bash
# 게이트웨이를 건너뛰면 토큰이 아예 필요 없다
curl -H "X-User-Id: 1" http://<node-4-주소>:8085/api/v1/orders
# → 1번 사용자의 주문 내역이 그대로 나온다
```

AWS 보안그룹이 이 포트를 막고 있다면 실제 노출은 없습니다. 다만 **코드와 컴포즈 어디에도 방어가 없고,
보안그룹 설정 하나에만 전적으로 의존**하는 상태입니다.

**대응 방법 (쉬운 것부터)**

1. compose에서 `ports:` 를 지웁니다. 같은 도커 네트워크 안에서는 포트를 노출하지 않아도 서로 통신됩니다.
   (`expose:` 만으로 충분하고, `expose`도 사실 불필요합니다.)
2. 게이트웨이가 공유 시크릿 헤더(예: `X-Gateway-Secret`)를 넣고, 각 서비스가 필터에서 그 값을 확인합니다.
3. 더 확실한 방법은 게이트웨이가 서비스로 넘길 때 **짧은 수명의 내부 토큰**을 새로 발급하는 것입니다
   (token exchange). 다만 이 규모에서는 1번만으로도 충분합니다.

### 6-2. 인가 목록과 경로 리라이트가 엇갈려서 생긴 실제 구멍

이건 조금 복잡하지만 **이 프로젝트에서 가장 중요한 발견**이라 단계별로 봅니다.

#### 먼저 알아야 할 것: 필터는 선언 순서대로 실행된다

```yaml
filters:
  - JwtAuthorization      # ← 먼저 실행 (order 1)
  - RewritePath=...       # ← 나중 실행 (order 2)
```

Spring Cloud Gateway는 라우트의 필터 목록을 **적힌 순서대로 order 1, 2, 3…** 을 붙여 실행합니다.
즉 **어느 걸 먼저 쓰느냐에 따라 JWT 필터가 보는 경로가 달라집니다.**

#### 실제로 배포되는 설정 (config repo의 `api-gateway-dev.yml`)

```yaml
# ① 동물/보호소 CUD — Jwt가 먼저, Rewrite가 나중
- id: animal-service-protected
  predicates:
    - Path=/api/animals/**, /api/shelters/**, ...
    - Method=POST,PUT,PATCH,DELETE
  filters:
    - JwtAuthorization                               # ← 먼저
    - RewritePath=/api/(?<segment>.*), /api/v1/${segment}

# ② 스토어 — Rewrite가 먼저, Jwt가 나중
- id: store-service
  predicates:
    - Path=/api/products/**, /api/carts/**, ...
  filters:
    - RewritePath=/api/(?<segment>.*), /api/v1/${segment}   # ← 먼저
    - JwtAuthorization
```

#### 이제 인가 목록과 맞춰 봅니다

```java
ADMIN_ONLY_PATHS = List.of(
    "POST:/api/v1/shelters",     // ← v1 있음
    "POST:/api/products",        // ← v1 없음
    ...);
NON_USER_PATHS = List.of(
    "POST:/api/v1/animals",      // ← v1 있음
    ...);
```

**① 보호소 등록 — `POST /api/shelters`**

| 단계 | 경로 |
|---|---|
| 클라이언트가 보냄 | `/api/shelters` |
| JwtAuthorization이 보는 경로 | `/api/shelters` ← **아직 리라이트 안 됨** |
| 목록에 등록된 패턴 | `POST:/api/v1/shelters` |
| 결과 | **매칭 실패 → 권한 검사 통과** |
| RewritePath 후 | `/api/v1/shelters` → animal-service로 전달 |

→ **로그인만 한 일반 회원(ROLE_USER)이 보호소를 등록할 수 있습니다.**
animal-service는 자체 권한 검사가 없습니다(컨트롤러 주석에 *"ROLE_ADMIN만 접근 가능 (API Gateway에서 체크)"*
라고만 적혀 있음).

같은 이유로 `POST /api/animals`, `DELETE /api/animals/5` 도 ROLE_USER가 호출할 수 있습니다.

**② 상품 등록 — `POST /api/products`**

| 단계 | 경로 |
|---|---|
| 클라이언트가 보냄 | `/api/products` |
| RewritePath가 먼저 실행 | `/api/v1/products` |
| JwtAuthorization이 보는 경로 | `/api/v1/products` ← **이미 리라이트됨** |
| 목록에 등록된 패턴 | `POST:/api/products` (v1 없음) |
| 결과 | **매칭 실패 → 권한 검사 통과** |

→ **일반 회원이 상품·카테고리·옵션그룹을 등록·수정·삭제할 수 있습니다.**

**즉 두 라우트가 정반대 이유로 똑같이 뚫렸습니다.** ①은 목록에 v1을 넣었는데 필터가 v1 없는 경로를 보고,
②는 목록에 v1을 안 넣었는데 필터가 v1 있는 경로를 봅니다.

**왜 이런 일이 생겼나** — 근본 원인은 **경로 체계가 두 개**라는 것입니다.
프론트엔드는 `/api/...`로 부르고 서비스는 `/api/v1/...`로 받습니다. 그 간극을 `RewritePath`로 메우면서
"필터가 리라이트 전 경로를 보는지 후 경로를 보는지"가 라우트마다 달라졌습니다.
코드의 주석 *"프론트엔드 요청 기준, v1 없음"* 이 그 혼란의 흔적입니다.

#### 무엇이 다행인가

`/api/v1/admin/**` 접두사 규칙([3-7](#3-7-관리자-경로만-별도-처리))은 **정상 작동합니다.**
모든 관리자 라우트가 `RewritePath`로 `/api/v1/admin/...`을 만든 뒤 JWT 필터를 타기 때문입니다.
회원 관리, 게시글 관리, 주문 관리, 통계는 제대로 막혀 있습니다.

#### 고치는 방법

**임시 처방 (5분)** — 리스트에 두 버전을 모두 넣습니다.

```java
"POST:/api/shelters", "POST:/api/v1/shelters",
"POST:/api/products", "POST:/api/v1/products",
```

**근본 처방 (권장)** — `/api/v1/admin/**` 방식을 확장합니다. 경로 접두사로 권한을 표현하면
목록 관리 자체가 사라집니다.

```java
// 예: 쓰기 작업은 무조건 접두사로 구분
if (pathMatcher.match("/**/admin/**", path))   return requireRole("ROLE_ADMIN");
if (pathMatcher.match("/**/manage/**", path))  return requireRole("ROLE_SHELTER", "ROLE_ADMIN");
```

**더 근본적으로** — 프론트도 `/api/v1/`을 쓰게 통일하면 `RewritePath`가 전부 사라지고,
이 종류의 버그가 구조적으로 발생하지 않습니다.

---

## 7. 함정: 코드의 라우팅 규칙은 실제로 쓰이지 않는다

`api-gateway/src/main/resources/application.yml`과 config repo의 `api-gateway-dev.yml`은
**내용이 다릅니다.** 그리고 배포 환경(`SPRING_PROFILES_ACTIVE=dev`)에서는 **config repo 쪽이 이깁니다.**

주요 차이:

| | 코드의 application.yml | config repo의 api-gateway-dev.yml (실제 적용) |
|---|---|---|
| user 경로 | `/api/v1/users/**` (리라이트 없음) | `/api/users/**` + `/api/v1/` 리라이트 |
| 이메일 경로 | `/api/v1/email/**` | `/api/email/**` + 리라이트 |
| 스토어 라우트 | products / orders 따로 | 하나로 묶음(`carts`, `wishlists`, `images` 포함) |
| 관리자 주문 | user-service 쪽에만 | `admin-orders` → store-service 추가 |
| CORS 허용 도메인 | localhost:3000/5173 + pawbridge.com | **pawbridge.kr** 만 |
| OPTIONS 처리 | Security에서 permitAll | `cors-preflight` 라우트로 204 반환 |
| JWT 시크릿 | 환경변수 | `{cipher}` 암호문 |

**작업할 때의 규칙**

- 로컬에서 띄워 테스트 → 코드의 `application.yml`
- 실제 배포 동작을 바꾸려면 → **`pawbridge-config-repo`를 수정하고 커밋**
- 두 파일이 계속 벌어지면 로컬에서 재현되지 않는 버그가 생깁니다. 라우팅은 config repo 한 곳만 두고
  로컬은 프로필로 구분하는 편이 안전합니다.

---

## 8. 이 문서에서 배울 개념 정리

| 개념 | 한 줄 설명 |
|---|---|
| **API Gateway** | 여러 서비스 앞에 두는 단일 입구. 라우팅·인증·CORS를 한곳에 모은다 |
| **Predicate / Filter** | 조건으로 라우트를 고르고, 필터로 요청을 가공한다 |
| **라우트 순서** | 위에서 아래로, 첫 일치가 이긴다. `Method` 조건 없는 라우트를 위에 두면 안 된다 |
| **필터 선언 순서** | 적힌 순서대로 실행 → 리라이트 전/후 경로가 달라진다 (6-2의 원인) |
| **WebFlux / `Mono<Void>`** | 비동기 논블로킹. "값"이 아니라 "작업 계획"을 반환한다 |
| **불변 요청 + `mutate()`** | 요청 객체는 못 바꾸므로 변경된 복사본을 만들어 넘긴다 |
| **중앙 인가 + 헤더 전파** | 게이트웨이가 JWT를 `X-User-*` 헤더로 번역. 중복 제거의 대가는 우회 위험 |
| **401 vs 403** | 신원 불명 vs 권한 부족. 프론트 동작이 완전히 다르다 |
| **AntPathMatcher** | `*`는 한 세그먼트, `**`는 여러 세그먼트 |
| **`allowedOriginPatterns`** | `allowCredentials: true`와 `*`는 공존 불가 → 패턴 매칭으로 해결 |
| **CSRF 비활성화** | 세션 기반 방어라 stateless에선 불필요. 단 쿠키를 쓰면 위험은 남는다 |
| **HMAC 대칭키** | 발급/검증 같은 키. 검증만 하는 쪽도 발급 능력을 갖는 단점 |

---

**다음 문서** → [03-user-service.md](03-user-service.md) — 인증의 심장부, 그리고 Outbox 패턴의 시작점
