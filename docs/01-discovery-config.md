# 01. discovery-service & config-service — MSA의 뼈대 두 개

> 코드는 각각 파일 2개, 3개뿐입니다. 그런데 이 두 서비스가 없으면 나머지 7개가 전부 뜨지 않습니다.
> 왜 그런지, 그리고 이 작은 코드가 실제로 무슨 일을 하는지 봅니다.

---

## 0. 먼저: 이 두 서비스가 왜 필요한가

MSA로 쪼개면 곧바로 두 가지 문제가 생깁니다.

**문제 1 — "상대방 주소를 어떻게 알지?"**

모놀리스라면 `userService.findUser()`처럼 메서드 호출입니다. 쪼개면 HTTP 호출이 되고, 주소가 필요합니다.
그런데 주소를 코드에 박으면(`http://10.0.1.23:8081`) 서버 IP가 바뀔 때마다 전 서비스를 재배포해야 합니다.
컨테이너는 재시작할 때마다 IP가 바뀔 수도 있습니다.

→ **discovery-service (Eureka)** 가 "이름 ↔ 주소" 전화번호부 역할을 합니다.

**문제 2 — "설정을 어디에 두지?"**

DB 비밀번호, JWT 시크릿, Kafka 주소… 이걸 서비스마다 `application.yml`에 적으면 7곳에 흩어집니다.
JWT 시크릿 하나 바꾸려면 7개 서비스를 다시 빌드해야 합니다. 게다가 비밀값이 git에 올라갑니다.

→ **config-service (Spring Cloud Config Server)** 가 설정을 한곳에 모아 나눠줍니다.

---

## 1. discovery-service — 서비스 전화번호부

### 코드 전체가 이것뿐입니다

[DiscoveryServiceApplication.java](../discovery-service/src/main/java/com/pawbridge/discoveryservice/DiscoveryServiceApplication.java)

```java
@EnableEurekaServer      // ← 이 한 줄이 Eureka 서버를 띄운다
@SpringBootApplication
public class DiscoveryServiceApplication { ... }
```

`@EnableEurekaServer` 하나로 등록/조회 API, 대시보드(`http://호스트:8761`)까지 전부 자동 구성됩니다.
직접 작성한 로직은 0줄입니다. **의존성 추가 + 애노테이션 = 서버 완성**이 Spring Cloud의 방식입니다.

### 실제로 벌어지는 일

```
① 서비스 시작    user-service → Eureka: "나는 user-service, 주소는 10.0.4.11:8081"
② 30초마다      user-service → Eureka: "나 살아있음" (heartbeat)
③ 호출할 때     gateway → Eureka: "user-service 주소 줘" → [10.0.4.11:8081]
④ 죽으면        heartbeat 끊김 → Eureka가 목록에서 제거 → 더 이상 그 주소로 안 보냄
```

게이트웨이 설정의 `uri: lb://user-service`에서 **`lb://`** 가 바로 이걸 씁니다.
"lb"는 load balancer — Eureka에서 받은 주소 목록을 라운드로빈으로 돌립니다.
user-service를 3대로 늘려도 게이트웨이 설정은 그대로입니다. 3대가 각자 등록하면 자동으로 3대에 분산됩니다.

### 설정에서 눈여겨볼 두 줄

[discovery-service/src/main/resources/application.yml](../discovery-service/src/main/resources/application.yml)

```yaml
eureka:
  client:
    register-with-eureka: false   # ① 자기 자신을 등록하지 않음
    fetch-registry: false          # ② 목록을 가져오지도 않음
  server:
    enable-self-preservation: false      # ③
    eviction-interval-timer-in-ms: 3000  # ④
```

**① ②** — Eureka 서버도 라이브러리상으로는 클라이언트 기능을 함께 갖고 있습니다. 서버가 여러 대일 때는 서로를
등록해서 목록을 복제하지만(peer awareness), 지금은 1대뿐이므로 껐습니다. 안 끄면 자기 자신에게 등록을
시도하다 시작 로그에 계속 에러가 찍힙니다.

**③ self-preservation(자기 보호 모드)** — 기본값은 `true`입니다. 이 모드는 "갑자기 heartbeat가 대량으로
끊기면, 서비스가 죽은 게 아니라 **네트워크가 끊긴 것**일 수 있다"고 판단해 목록을 지우지 않고 붙잡아 둡니다.
운영에서는 안전장치지만, 개발 중에는 **이미 죽은 서비스가 목록에 계속 남아 요청이 그쪽으로 가서 실패**합니다.
그래서 껐습니다.

**④** — 죽은 항목을 청소하는 주기를 3초로 줄였습니다. 기본은 60초라 개발 중 답답합니다.

> ⚠️ **운영 관점의 주의**: ③④는 "개발 편의" 설정입니다. 그런데 이 파일은 프로필과 무관하게 항상 적용되고,
> 실제 배포도 이 값으로 돌고 있습니다. 노드 간 네트워크가 잠깐 끊기면 Eureka가 멀쩡한 서비스를 전부
> 목록에서 지워버릴 수 있습니다. 운영에서는 `enable-self-preservation: true`가 맞습니다.

---

## 2. config-service — 설정 배급소

### 코드

[ConfigServiceApplication.java](../config-service/src/main/java/com/pawbridge/configservice/ConfigServiceApplication.java)

```java
@SpringBootApplication
@EnableConfigServer     // ← 이 한 줄
```

역시 한 줄입니다. 실제 설정 내용은 **git 저장소**에 있고, config-service는 그걸 읽어 HTTP로 나눠주는
중계자입니다.

### 설정 파일은 이 레포에 없습니다

[config-service/src/main/resources/application.yml](../config-service/src/main/resources/application.yml)

```yaml
spring:
  cloud:
    config:
      server:
        git:
          uri: ${CONFIG_REPO_URI}        # ← 별도의 git 저장소
          default-label: main
          username: ${CONFIG_REPO_USERNAME}
          password: ${CONFIG_REPO_TOKEN}
          clone-on-start: true
```

이 프로젝트의 실제 설정은 **`pawbridge-config-repo`라는 별개의 git 레포**에 있습니다:

```
pawbridge-config-repo/
├── application.yml            ← 모든 서비스 공통 (JPA, actuator, 로깅)
├── application-dev.yml        ← 모든 서비스 공통 + dev 프로필 (Kafka, Redis, Eureka, Zipkin 주소)
├── user-service-dev.yml       ← user-service + dev 프로필
├── animal-service-dev.yml
├── api-gateway-dev.yml        ← ★ 게이트웨이 라우팅 규칙이 여기 있다
└── ... (서비스 × 프로필)
```

**이게 중요합니다.** `api-gateway/src/main/resources/application.yml`에도 라우팅 규칙이 적혀 있지만,
실제 배포(dev 프로필)에서 적용되는 건 **config repo의 `api-gateway-dev.yml`** 입니다.
Config Server가 준 설정이 앱 자기 `application.yml`보다 **우선순위가 높기** 때문입니다.

> 📌 **처음 이 프로젝트를 보는 사람이 가장 많이 헤매는 지점입니다.**
> "코드의 라우팅 규칙을 고쳤는데 배포하면 안 바뀐다" → 고칠 곳은 config repo입니다.
> 자세한 비교는 [02-api-gateway.md](02-api-gateway.md#7-함정-코드의-라우팅-규칙은-실제로-쓰이지-않는다)에 있습니다.

### 파일 이름 규칙 — 왜 `user-service-dev.yml`인가

Config Server는 요청 URL을 `{애플리케이션이름}/{프로필}`로 받아 다음 순서로 합칩니다(뒤에 오는 게 이김):

```
1. application.yml          ← 전 서비스 공통
2. application-dev.yml      ← 전 서비스 공통, dev 한정
3. user-service.yml         ← user-service 전용 (이 프로젝트엔 없음)
4. user-service-dev.yml     ← user-service 전용, dev 한정  ★ 가장 강함
```

그래서 "Kafka 주소는 모든 서비스가 같으니 `application-dev.yml`에", "DB 이름은 서비스마다 다르니
`user-service-dev.yml`에" 하고 나눈 것입니다. 중복이 사라집니다.

### `{cipher}` — 비밀값을 git에 올리는 방법

config repo를 열어 보면 이런 값이 보입니다:

```yaml
  datasource:
    username: '{cipher}cfa8931fbb7f37f3c00c4cadf425e89481b98e2d7a1ef06a529db3c4f300c516'
    password: '{cipher}ffffff021ac191e145907a7d410ba11fce9048f885126f88688afabc4e07f568'
```

**동작 원리 (Spring Cloud Config의 대칭키 암호화)**

```
① 사람이 미리 암호화:  POST /encrypt  "myPassword"  →  "a1b2c3..."
② git에 커밋:          password: '{cipher}a1b2c3...'
③ 서비스가 설정 요청
④ Config Server가 encrypt.key로 복호화해서 평문으로 내려줌
⑤ 서비스는 평문 "myPassword"를 받는다 (자기가 복호화하는 게 아님)
```

복호화 키는 config-service의 `encrypt.key`(= 환경변수 `CONFIG_ENCRYPT_KEY`)에만 있습니다.
**즉 config repo가 유출돼도 이 키가 없으면 암호문은 쓸모없습니다.** 그래서 설정 레포를 git으로 관리할 수
있게 된 것입니다.

이 프로젝트에서 `{cipher}`로 감싼 것들: DB 계정, Gmail SMTP 계정/앱 비밀번호, Google OAuth2 client-id/secret,
JWT 시크릿, AWS S3 액세스 키, 공공데이터포털 서비스 키, 토스 시크릿 키.

### Config Server 자체는 Basic 인증으로 막혀 있다

[config-service/config/SecurityConfig.java](../config-service/src/main/java/com/pawbridge/configservice/config/SecurityConfig.java)

```java
.authorizeHttpRequests(auth -> auth
    .requestMatchers("/actuator/health").permitAll()  // 헬스체크만 열어둠
    .anyRequest().authenticated()
)
.httpBasic(Customizer.withDefaults());
```

당연한 조치입니다. Config Server는 복호화된 **평문 설정을 HTTP로 내려주는** 서버입니다.
아무나 `GET /user-service/dev`를 부르면 DB 비밀번호가 그대로 나옵니다.
그래서 아이디/비밀번호(`CONFIG_USERNAME`/`CONFIG_PASSWORD`)를 요구하고, 각 서비스는 이렇게 붙습니다:

```yaml
# 각 서비스의 application.yml
spring:
  config:
    import: "configserver:http://${CONFIG_SERVER_HOST}"
  cloud:
    config:
      username: ${CONFIG_USERNAME}
      password: ${CONFIG_PASSWORD}
```

단, `/actuator/health`만 열어둔 것은 도커 헬스체크와 로드밸런서가 인증 없이 상태를 확인해야 하기 때문입니다.

### `fail-fast`와 `retry` — 시작 순서 문제를 푸는 방법

각 서비스 설정에 공통으로 이게 붙어 있습니다:

```yaml
spring:
  cloud:
    config:
      fail-fast: true            # 설정을 못 받으면 그냥 죽어라
      retry:
        max-attempts: 6          # 대신 6번까지 재시도
        initial-interval: 1000   # 1초 간격에서 시작 (지수적으로 늘어남)
```

**왜 이렇게 하나?**

`fail-fast: false`(기본값)면 Config Server가 죽어 있어도 서비스가 **일단 뜹니다.** 그런데 DB 주소를 못
받았으니 첫 요청에서 알 수 없는 에러가 납니다. 원인을 찾기가 매우 어렵습니다.

`fail-fast: true`면 시작 자체가 실패하고 로그에 "Config Server 접속 실패"가 명확히 찍힙니다.
**"조용히 잘못 동작하는 것보다 시끄럽게 죽는 게 낫다"** — 분산 시스템의 기본 원칙입니다.

그런데 컨테이너를 동시에 올리면 Config Server가 아직 준비되지 않았을 수 있습니다. 그래서 `retry`를 붙여
1초 → 2초 → 4초… 로 6번 기다립니다. **fail-fast의 명확함은 유지하면서 시작 순서 문제만 해결**하는 조합입니다.

### 왜 config-service는 Eureka에 등록하지 않나

`config-service/build.gradle`에는 eureka-client 의존성이 없습니다. 다른 서비스들은 Config Server를
`${CONFIG_SERVER_HOST}`라는 **고정 주소**로 찾습니다.

이유는 순서입니다. Eureka로 config-service를 찾으려면 → Eureka 주소를 알아야 하고 → Eureka 주소는
설정에 있고 → 그 설정은 Config Server에 있습니다. **닭과 달걀**입니다.
그래서 부트스트랩의 맨 처음 한 단계는 고정 주소로 시작해야 합니다.

---

## 3. 여기서 배울 개념 정리

| 개념 | 한 줄 설명 | 이 프로젝트에서 |
|---|---|---|
| **Service Discovery** | 주소가 아니라 이름으로 호출 | `lb://user-service` |
| **Heartbeat / Eviction** | 살아있음 신호가 끊기면 목록에서 제거 | 3초 주기로 청소 |
| **Self-preservation** | 대량 신호 유실 시 "네트워크 문제"로 보고 목록 보존 | 개발 편의로 껐음(운영에선 위험) |
| **설정 외부화** | 설정을 코드에서 분리해 재배포 없이 바꾼다 | 별도 git 레포 |
| **설정 우선순위** | Config Server 설정 > 앱 내부 application.yml | 실제 라우팅은 config repo에 |
| **`{cipher}` 대칭키 암호화** | 암호문만 git에 올리고 서버가 복호화 | DB·JWT·OAuth·S3 키 |
| **fail-fast + retry** | 조용한 오작동 대신 시끄러운 실패, 단 재시도는 허용 | 6회 재시도 |
| **부트스트랩 순서** | Config Server만은 고정 주소여야 한다 | `CONFIG_SERVER_HOST` |

---

## 4. 개선하면 좋을 점

1. **`enable-self-preservation: false`는 운영에서 위험합니다.** 노드 간 네트워크가 순간 끊기면 정상
   서비스들이 전부 목록에서 사라집니다. 프로필로 분리해 dev만 끄는 게 맞습니다.

2. **Eureka와 Config Server가 단일 인스턴스입니다.** 둘 다 죽으면 전체가 영향받습니다.
   Eureka는 여러 대를 서로 등록시켜(peer awareness) 이중화할 수 있고, Config Server는 무상태이므로
   같은 이미지를 2대 띄우면 됩니다. (다만 지금 규모에서는 합리적인 타협입니다.)

3. **`spring-cloud-starter-bootstrap` 의존성이 남아 있습니다.**
   discovery-service와 api-gateway에 이 의존성이 있는데, 동시에 새 방식인
   `spring.config.import: configserver:`도 쓰고 있습니다. 둘은 서로 다른 시대의 방식이라 하나만 쓰는
   편이 혼란이 적습니다(Spring Boot 2.4+에서는 `spring.config.import` 권장).

---

**다음 문서** → [02-api-gateway.md](02-api-gateway.md) — 모든 요청이 지나가는 단 하나의 문
