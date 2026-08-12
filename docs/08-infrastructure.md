# 08. infrastructure — Kafka·Debezium·Elasticsearch·5노드 배포·모니터링

> 여기까지 각 서비스의 코드를 봤습니다. 이 문서는 **그 코드들이 실제로 어디서 어떻게 도는지**를 다룹니다.
> 서비스 코드보다 이해하기 어렵고, 문제가 생겼을 때 원인을 찾기도 어려운 영역입니다.

```
infrastructure/
├── mysql/          docker-compose + init-sql/init.sh   (5개 스키마 생성)
├── kafka/          docker-compose + Dockerfile + connectors/ (8개)
├── elasticsearch/  docker-compose + Dockerfile(nori) + mappings/ + setup 스크립트
├── redis/          docker-compose
├── monitoring/     docker-compose + prometheus/ + grafana/provisioning/   ← 로컬용
├── prod/           prometheus/ + grafana/ + connectors/                   ← 운영용
├── start-all.sh / .bat, stop-all.sh / .bat
├── setup_prod_cdc.sh
└── README.md

deployment/
├── docker-compose-node-1.yml ~ node-5.yml
└── nginx.conf

.github/workflows/    deploy-{서비스}.yml 10개
```

---

## 1. 5노드 배포 지도

EC2 5대에 역할을 나눠 담았습니다.

```
                         인터넷
                            │ 443
┌───────────────────────────▼──────────────────────────────────────┐
│ Node-1  "관문 + 관측"                                              │
│   nginx (TLS 종료)  →  api-gateway:8080                            │
│   discovery-service:8761 (Eureka)                                  │
│   redis:6379                                                       │
│   prometheus:9090 / grafana:3000 / zipkin:9411 / node-exporter      │
└────────────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────────────┐
│ Node-2  "설정 + 데이터"                                             │
│   config-service:8888                                              │
│   mysql:3306   ← pawbridge_{user,animal,community,store,payment}   │
│   kibana:5601                                                      │
└────────────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────────────┐
│ Node-3  "메시징 + 동물"                                             │
│   zookeeper:2181 / kafka:9092,29092                                │
│   animal-service:9020   ← 배치가 무거워서 따로 뺐다                   │
└────────────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────────────┐
│ Node-4  "애플리케이션"                                              │
│   user-service:8081  community-service:8089                         │
│   store-service:8085 payment-service:8084                           │
└────────────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────────────┐
│ Node-5  "검색"                                                      │
│   elasticsearch:9200 (nori 플러그인 포함 커스텀 이미지)               │
│   kafka-connect:8083 (Debezium + ES Sink)                          │
└────────────────────────────────────────────────────────────────────┘
```

### 왜 이렇게 나눴나 — 짐작이 아니라 근거가 보입니다

| 결정 | 이유 |
|---|---|
| **animal-service를 Kafka와 같은 노드에** | 새벽 배치가 수만 건을 처리해 CPU·메모리를 많이 씀 → 다른 앱과 분리 |
| **Elasticsearch를 단독 노드에** | JVM 힙을 크게 쓰고 디스크 I/O가 많음. 다른 것과 경쟁하면 검색이 느려짐 |
| **kafka-connect를 ES와 같은 노드에** | CDC 싱크가 ES에 대량 쓰기를 하므로 네트워크 왕복을 줄임 |
| **MySQL을 config-service와 같은 노드에** | 둘 다 "다른 서비스가 시작할 때 필요한" 것 → 함께 먼저 뜨면 편함 |
| **redis를 gateway와 같은 노드에** | 캐시·세션은 지연이 짧아야 함. 다만 사용자는 node-4에 있어 한 홉 건너감 |
| **nginx만 인터넷에 노출** | 나머지는 VPC 내부. 공격 표면을 하나로 좁힘 |

**의도가 분명한 배치입니다.** "그냥 다섯 개에 나눠 담은" 것이 아니라 자원 특성을 고려했습니다.

### ⚠️ 단일 장애점이 여럿입니다

```
Node-2 죽으면 → MySQL 전멸 → 5개 서비스 전부 정지
                Config Server 정지 → 새 컨테이너가 뜨지 못함 (fail-fast)
Node-3 죽으면 → Kafka 정지 → 모든 이벤트 전파 중단 (단, Outbox에 쌓이므로 유실은 아님)
Node-1 죽으면 → nginx·게이트웨이·Eureka 전멸 → 외부 접근 완전 차단
```

Kafka는 **replication factor가 1**입니다.

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

브로커가 1대이므로 당연한 설정이지만, **브로커 디스크가 깨지면 미처리 이벤트가 사라집니다.**
다행히 Outbox 테이블이 MySQL에 남아 있어 재발행은 가능합니다.
**Outbox 패턴이 인프라 이중화 부족을 일부 보완하고 있는 셈입니다.**

학습·포트폴리오 규모에서는 합리적인 타협입니다. 다만 **알고 있는 것과 모르는 것은 다릅니다.**

---

## 2. nginx — TLS 종료와 단일 진입점

[deployment/nginx.conf](../deployment/nginx.conf)

```nginx
# 1. HTTP(80) → HTTPS(443) 강제 리다이렉트
server {
    listen 80;
    server_name pawbridge.kr www.pawbridge.kr api.pawbridge.kr;
    return 301 https://$host$request_uri;
}

# 2. 백엔드 API (api.pawbridge.kr) → Gateway로 전달
server {
    listen 443 ssl;
    server_name api.pawbridge.kr;
    ssl_certificate     /etc/letsencrypt/live/pawbridge.kr/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/pawbridge.kr/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    location / {
        proxy_pass http://api-gateway;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### TLS 종료(TLS Termination)란

```
브라우저 ──[HTTPS 암호화]──► nginx ──[HTTP 평문]──► api-gateway
                              ↑
                     여기서 암호를 풀고 내부는 평문으로
```

**왜 이렇게 하나**

- 인증서를 **한 곳에서만** 관리합니다. 서비스 7개에 각각 인증서를 넣고 갱신하는 것은 비현실적입니다.
- TLS 암복호화는 CPU를 씁니다. nginx가 그 일을 전담하면 애플리케이션은 순수 로직만 처리합니다.
- Let's Encrypt 갱신(certbot)도 한 곳에서 끝납니다.

**전달하는 헤더의 의미**

| 헤더 | 없으면 무슨 일이 생기나 |
|---|---|
| `Host` | 게이트웨이가 원래 도메인을 몰라 리다이렉트 URL이 깨진다 |
| `X-Real-IP` / `X-Forwarded-For` | 모든 요청의 IP가 nginx로 보인다 → 로그·차단이 무의미 |
| `X-Forwarded-Proto` | 애플리케이션이 HTTP로 착각해 `http://`로 리다이렉트 → 무한 루프 |
| `Upgrade` / `Connection` | WebSocket 연결이 성립하지 않는다 |

**`X-Forwarded-Proto`가 특히 중요합니다.** 이게 없으면 OAuth2 리다이렉트 URL이 `http://`로
생성되어 구글 로그인이 실패하는, 원인을 찾기 어려운 문제가 생깁니다.

⚠️ 다만 [03-user-service.md 5절](03-user-service.md#-문제-prod-분기가-실제로는-실행되지-않습니다)에서 본 대로,
쿠키에 `Secure` 플래그가 붙지 않으므로 **80 → 443 리다이렉트 직전의 평문 요청에 쿠키가 실려 나갈 수
있습니다.** nginx가 리다이렉트를 해도 첫 요청은 이미 평문으로 전송된 뒤입니다.

---

## 3. Kafka — 명시적 토픽 생성과 자동 커넥터 등록

[infrastructure/kafka/docker-compose.yml](../infrastructure/kafka/docker-compose.yml)

### 3-1. 리스너 두 개 — 컨테이너 안팎에서 같은 Kafka를 쓰는 방법

```yaml
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-broker:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:29092,PLAINTEXT_HOST://0.0.0.0:9092
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
```

Kafka를 도커로 띄울 때 가장 많이 막히는 부분입니다.

**문제**: Kafka 클라이언트는 처음에 아무 브로커에 접속해 **"브로커 목록"을 받아** 그 주소로 다시 접속합니다.
그 목록에 담기는 값이 `advertised.listeners`입니다.

```
컨테이너 안(kafka-connect)에서 접속 → "localhost:9092" 를 받으면? → 자기 자신을 찾다가 실패 💀
호스트(개발자 PC)에서 접속       → "kafka-broker:29092" 를 받으면? → DNS 해석 실패 💀
```

**해법: 리스너를 두 개 만들어 접속 경로별로 다른 주소를 알려줍니다.**

```
29092 (PLAINTEXT)       → "kafka-broker:29092"  ← 컨테이너끼리 (도커 네트워크 DNS)
 9092 (PLAINTEXT_HOST)  → "localhost:9092"      ← 호스트에서 접속
```

실제로 애플리케이션은 `${NODE3_PRIVATE_IP}:9092`로 접속하고
(config repo의 `application-dev.yml`), 커넥터는 `kafka-broker:29092`로 접속합니다
(커넥터 JSON의 `schema.history.internal.kafka.bootstrap.servers`).
**두 경로가 실제로 모두 쓰이고 있습니다.**

> ⚠️ `PLAINTEXT_HOST`의 advertised 주소가 `localhost:9092`인데, 애플리케이션은 다른 노드(node-4)에서
> `${NODE3_PRIVATE_IP}:9092`로 접속합니다. 최초 접속은 성공하지만 **브로커 목록으로 `localhost:9092`를
> 받으면 node-4의 자기 자신에 접속을 시도합니다.**
> 지금 동작하고 있다면 배포된 compose(`docker-compose-node-3.yml`)가 이 값을 사설 IP로 덮어쓰고 있을
> 것입니다. 이 파일(로컬용)과 배포용이 다른 값을 쓰는 셈이므로, 확인해 두는 게 좋습니다.

### 3-2. 토픽 자동 생성을 끄고 직접 만든다

```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false" # 명시적으로 생성하므로 false로 변경
```

**자동 생성을 끄는 것은 좋은 판단입니다.**

```
자동 생성이 켜져 있으면:
  코드에 토픽 이름을 "user.favorit.events" 라고 오타 내면
  → Kafka가 그 이름으로 토픽을 만들어 준다
  → 프로듀서는 성공하고 컨슈머는 영원히 아무것도 못 받는다
  → 에러가 없어서 원인 찾기가 매우 어렵다  💀

자동 생성이 꺼져 있으면:
  → 없는 토픽에 발행하면 즉시 실패 → 오타를 바로 발견
```

또한 자동 생성된 토픽은 **파티션 수·복제 수를 브로커 기본값으로** 가져갑니다.
파티션 1개로 만들어져 나중에 확장이 어려워집니다.

그래서 `kafka-topic-creator`라는 **일회성 컨테이너**로 토픽을 만듭니다.

```yaml
kafka-topic-creator:
  command: >
    bash -c "
      cub kafka-ready -b kafka-broker:29092 1 30 &&       # ★ Kafka 준비 대기
      kafka-topics --create --if-not-exists ... --topic connect_configs --config cleanup.policy=compact &&
      kafka-topics --create --if-not-exists ... --topic user.favorite.events &&
      kafka-topics --create --if-not-exists ... --topic user.compensation.events &&
      ...
    "
  restart: "no"                                            # ★ 한 번만 실행
```

**`cub kafka-ready`** 는 Confluent 이미지가 제공하는 대기 유틸리티입니다.
`depends_on`은 "컨테이너가 시작됐다"만 보장하고 "Kafka가 요청을 받을 준비가 됐다"는 보장하지 않습니다.
그래서 실제로 준비될 때까지 기다립니다. **컨테이너 오케스트레이션의 고전적 함정을 정확히 처리했습니다.**

**`--config cleanup.policy=compact`** 가 붙은 토픽(`connect_configs` 등)은 Kafka Connect의 내부
저장소입니다. compact 정책은 **같은 키의 마지막 값만 남깁니다.**
"현재 커넥터 설정"만 알면 되고 과거 이력은 필요 없으므로 적절합니다.

⚠️ **모든 토픽이 `--partitions 1`입니다.**
파티션이 1개면 **소비자를 여러 개 띄워도 하나만 일합니다**(한 파티션은 한 소비자에게만 할당).
지금은 서비스가 1대씩이라 문제없지만, **확장하려면 파티션을 먼저 늘려야 합니다.**
그리고 파티션을 늘리는 순간 [04-animal-service.md 5-10](04-animal-service.md#5-10--파티션-키에-관한-주석이-실제와-다릅니다)에서
지적한 동시성 문제가 실제로 발생합니다. **순서가 있습니다: 원자적 UPDATE로 고친 뒤 파티션을 늘려야 합니다.**

### 3-3. `connector-init` — 커넥터를 자동으로 등록하는 컨테이너

Kafka Connect는 커넥터를 **REST API로 등록**해야 동작합니다. 수동으로 `curl`을 8번 실행하는 대신
전용 컨테이너를 만들었습니다.

```yaml
connector-init:
  image: curlimages/curl:latest
  command:
    - sh
    - -c
    - |
      for i in $$(seq 1 36); do                              # ① Connect 준비 대기 (최대 3분)
        if curl -f -s http://kafka-connect:8083/connectors > /dev/null 2>&1; then break; fi
        sleep 5
      done
      CONNECTORS="community-outbox-connector.json animal-outbox-connector.json ..."
      for CONNECTOR_FILE in $$CONNECTORS; do                 # ② 순서대로 등록
        RESPONSE=$$(curl -s -w "\n%{http_code}" -X POST http://kafka-connect:8083/connectors \
          -H "Content-Type: application/json" -d @/connectors/$$CONNECTOR_FILE)
        HTTP_CODE=$$(echo "$$RESPONSE" | tail -n1)
        if   [ "$$HTTP_CODE" = "201" ]; then echo "✅ registered"
        elif [ "$$HTTP_CODE" = "409" ]; then echo "ℹ️  already exists"    # ③ 멱등 처리
        else echo "❌ Failed (HTTP $$HTTP_CODE)"; echo "$$BODY"; fi
      done
  restart: "no"
```

**세 가지가 잘 되어 있습니다.**

1. **준비 대기 루프** — Connect는 플러그인 로딩에 수십 초가 걸립니다. 무작정 POST하면 실패합니다.
2. **409를 정상으로 처리** — 이미 등록된 커넥터에 POST하면 409가 옵니다.
   이걸 실패로 보면 **재배포할 때마다 스크립트가 에러를 냅니다.** 멱등하게 만든 것이 정확합니다.
3. **실패 시 응답 본문 출력** — 커넥터 설정 오류는 응답 본문에 이유가 담깁니다. 그걸 로그로 남깁니다.

`$$`는 docker-compose가 `$`를 변수로 해석하지 않게 하는 이스케이프입니다.
`$i`라고 쓰면 compose가 자기 변수로 치환하려 합니다.

**이 방식의 의미**: "인프라를 코드로(Infrastructure as Code)" 관리하는 초보적이지만 실질적인 형태입니다.
`docker compose up` 한 번으로 Kafka + 토픽 + Connect + 커넥터 8개가 전부 구성됩니다.

⚠️ 다만 **커넥터 설정을 바꿔도 반영되지 않습니다.** 이미 존재하면 409로 건너뛰기 때문입니다.
설정 변경을 반영하려면 `PUT /connectors/{name}/config`를 써야 합니다.

```bash
# 개선: POST 대신 PUT 사용 (등록 + 갱신 겸용)
curl -X PUT http://kafka-connect:8083/connectors/$$NAME/config \
     -H "Content-Type: application/json" -d @config-only.json
```

실제로 커넥터 이름에 `-v3`, `-v5`가 붙은 것은 **설정을 바꾸려고 새 이름으로 다시 등록한 흔적**입니다.
`PUT`을 쓰면 그럴 필요가 없습니다.

### 3-4. Kafka Connect 커스텀 이미지

[infrastructure/kafka/Dockerfile](../infrastructure/kafka/Dockerfile)

```dockerfile
FROM confluentinc/cp-kafka-connect:7.5.0
RUN confluent-hub install --no-prompt debezium/debezium-connector-mysql:2.2.1
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:latest
```

기본 이미지에는 커넥터 플러그인이 없어서 직접 설치합니다.

⚠️ **`:latest`는 위험합니다.** 이미지를 다시 빌드하면 다른 버전이 설치되어 **어제는 되던 것이 오늘 안 될**
수 있습니다. Debezium은 `2.2.1`로 고정했는데 ES 싱크만 `latest`입니다. 버전을 명시해야 합니다.

---

## 4. ★ Debezium — CDC와 Outbox Event Router

이 프로젝트의 이벤트 전파 전부가 여기에 달려 있습니다.

### 4-1. CDC(Change Data Capture)란

```
[일반적인 방식]                        [CDC 방식]
애플리케이션이 DB에 쓰고               애플리케이션은 DB에만 쓴다
Kafka에도 직접 쏜다                    Debezium이 MySQL binlog를 읽어 Kafka로 보낸다
  → Dual Write 문제                     → 애플리케이션은 Kafka를 모른다
```

**binlog(Binary Log)** 는 MySQL이 모든 데이터 변경을 순서대로 기록하는 로그입니다.
본래 복제(replication)용인데, Debezium은 **자기를 MySQL의 복제 슬레이브처럼 등록**해서
그 스트림을 읽습니다.

```json
"database.server.id": "184057"
```

**`server.id`가 그래서 필요합니다.** MySQL 복제 구성에서 각 슬레이브는 고유한 ID를 가져야 합니다.
그리고 이 프로젝트는 커넥터마다 다른 값을 씁니다.

| 커넥터 | server.id |
|---|---|
| user-outbox-connector | 184057 |
| community-outbox-connector | 184055 |
| animal-outbox-connector | (별도) |
| animal-animals-connector | 777777 |
| store-outbox-connector-v3 | 2004 |
| payment-outbox-connector-v5 | 2002 |

**겹치면 두 커넥터가 서로를 끊어버립니다.** MySQL이 "같은 ID의 슬레이브가 접속했다"고 판단해
기존 연결을 종료시킵니다. 그러면 **CDC가 무한히 재접속하며 이벤트가 멈춥니다.**
다르게 부여한 것이 정확합니다.

### 4-2. EventRouter SMT — Outbox 행을 이벤트로 바꾸는 변환기

**SMT(Single Message Transform)** 는 Kafka Connect가 메시지 하나하나를 가공하는 장치입니다.
Debezium이 제공하는 `EventRouter`는 **Outbox 패턴 전용**입니다.

```json
"transforms": "outbox",
"transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
"transforms.outbox.table.field.event.id": "event_id",
"transforms.outbox.table.field.event.key": "aggregate_id",
"transforms.outbox.table.field.event.timestamp": "created_at",
"transforms.outbox.table.field.event.payload": "payload",
"transforms.outbox.table.fields.additional.placement": "event_type:header:eventType",
"transforms.outbox.route.by.field": "aggregate_type",
"transforms.outbox.route.topic.replacement": "user.favorite.events",
"transforms.outbox.table.expand.json.payload": "true"
```

**변환 없이 그냥 두면** Debezium은 이런 메시지를 보냅니다.

```json
{
  "before": null,
  "after": {
    "outbox_event_id": 42,
    "event_id": "a1b2-...",
    "aggregate_type": "Favorite",
    "aggregate_id": "1",
    "event_type": "FAVORITE_ADDED",
    "payload": "{\"userId\":1,\"animalId\":3}"
  },
  "source": { "db": "pawbridge_user", "table": "outbox_events", "ts_ms": ... },
  "op": "c"
}
```

소비자가 `after.payload`를 꺼내 문자열을 다시 파싱해야 합니다. **EventRouter가 이걸 대신 해 줍니다.**

```json
// 변환 후 — 순수한 도메인 이벤트
{ "eventType": "FAVORITE_ADDED", "eventId": "a1b2-...", "userId": 1, "animalId": 3 }

// 헤더:  id = "a1b2-..."        (table.field.event.id)
//        eventType = "FAVORITE_ADDED"  (additional.placement)
// 키:    "1"                    (table.field.event.key = aggregate_id)
// 토픽:  user.favorite.events   (route.topic.replacement)
```

**각 설정이 하는 일**

| 설정 | 하는 일 | 왜 중요한가 |
|---|---|---|
| `table.field.event.payload` | 이 컬럼을 메시지 본문으로 | 나머지 컬럼은 버려진다 |
| `table.expand.json.payload` | JSON 문자열을 객체로 펼침 | 소비자가 두 번 파싱하지 않아도 된다 |
| `table.field.event.key` | 이 컬럼을 **Kafka 메시지 키**로 | **같은 키 = 같은 파티션 = 순서 보장** ★ |
| `table.field.event.id` | 이 값을 헤더 `id`로 | 소비자의 멱등성 판단 근거 |
| `additional.placement` | 지정 컬럼을 헤더로 이동 | 본문을 순수하게 유지 |
| `route.by.field` | 이 컬럼 값으로 토픽 결정 | 한 테이블 → 여러 토픽 |
| `route.topic.replacement` | 최종 토픽 이름 | 고정값이면 항상 이 토픽 |

**`route.by.field`와 `route.topic.replacement`의 관계가 헷갈립니다.**
`route.topic.replacement`에 `${routedByValue}`를 쓰면 `aggregate_type` 값이 토픽 이름에 들어갑니다.
이 프로젝트는 **고정 문자열**을 넣었으므로, `aggregate_type`이 무엇이든 **전부 같은 토픽으로 갑니다.**

그 결과가 store-service에서 드러납니다.

```
store 의 outbox 테이블에 두 종류가 섞여 들어간다
  aggregate_type = "ORDER"        (ORDER_PAID)      ← OrderServiceImpl
  aggregate_type = "PRODUCT_SKU"  (SKU_UPDATED)     ← ProductOutboxService
        ↓ 둘 다
  토픽: store.outbox.events
        ↓ ES 싱크가 그대로 색인
  인덱스: store.outbox.events   ← 주문 이벤트와 상품 문서가 한 인덱스에 섞인다
```

**상품 검색 인덱스에 주문 결제 이벤트가 들어갑니다.** 검색 결과에 이상한 문서가 섞이거나,
매핑 충돌로 색인이 실패할 수 있습니다.

**고치는 방법**

```json
// aggregate_type 별로 토픽을 분리한다
"transforms.outbox.route.topic.replacement": "store.${routedByValue}.events"
// → store.ORDER.events / store.PRODUCT_SKU.events
// 그러면 ES 싱크는 상품 토픽만 구독하면 된다
```

### 4-3. 커넥터 8개 지도

```
[MySQL: pawbridge_user]      outbox_events ──user-outbox-connector──► user.favorite.events
                                                                       └► animal-service 소비 (찜 카운트)
                                          ──(같은 커넥터)────────────► user.compensation.events ✗
[MySQL: pawbridge_animal]    outbox_events ──animal-outbox-connector──► user.compensation.events
                                                                       └► user-service 소비 (보상)
                             animals       ──animal-animals-connector─► animal.animals
                                                                       └► elasticsearch-sink-v3 ─► ES "animals"
[MySQL: pawbridge_community] outbox_events ──community-outbox-connector► community.post.events
                                                                       └► community-service 소비 (ES 색인)
[MySQL: pawbridge_store]     outbox      ──store-outbox-connector-v3──► store.outbox.events
                                                                       └► store-es-sink-connector ─► ES
[MySQL: pawbridge_payment]   outbox      ──payment-outbox-connector-v5► payment.events
                                                                       └► store-service 소비 (주문 확정)
```

**두 종류가 섞여 있다는 점이 중요합니다.**

- **Outbox 커넥터 (5개)** — `outbox` 테이블을 읽어 도메인 이벤트로 변환. EventRouter SMT 사용.
- **테이블 CDC 커넥터 (1개)** — `animals` 테이블을 **직접** 읽음. 검색 인덱스 동기화용.
- **ES 싱크 커넥터 (3개)** — Kafka → Elasticsearch 방향.

`animal-animals-connector`는 SMT가 다릅니다.

```json
"transforms": "renameTopic",
"transforms.renameTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
"transforms.renameTopic.regex": "animal.pawbridge_animal.(.*)",
"transforms.renameTopic.replacement": "animal.$1"
```

Debezium은 기본적으로 `{topic.prefix}.{db}.{table}` 형태로 토픽 이름을 만듭니다
(`animal.pawbridge_animal.animals`). DB 이름이 들어가 있어 지저분하므로
**정규식으로 `animal.animals`로 줄였습니다.** `RegexRouter`는 이런 용도의 범용 SMT입니다.

### 4-4. ⚠️ animal-service의 ES 동기화가 두 경로로 겹칩니다

`animals` 인덱스를 채우는 방법이 **두 개** 있습니다.

```
경로 ①  배치 Step 2:  MySQL 전체 조회 → deleteAllDocuments() → 전체 재색인
        (매일 새벽 2시, ElasticsearchIndexService.reindexAllAnimals)

경로 ②  CDC:  animals 테이블 변경 → animal.animals 토픽 → elasticsearch-sink-v3 → ES
        (실시간)
```

**두 경로가 같은 인덱스에 씁니다.** 그리고 ES 싱크 설정이 이렇습니다.

```json
"key.ignore": "true",
"write.method": "upsert",
"schema.ignore": "true"
```

**`key.ignore: true`는 "Kafka 메시지 키를 문서 ID로 쓰지 않는다"는 뜻입니다.**
그러면 Confluent ES 싱크는 문서 ID를 `{토픽}+{파티션}+{오프셋}` 으로 생성합니다.

```
같은 3번 동물을 5번 수정하면:
  animal.animals+0+100  → 문서 1
  animal.animals+0+101  → 문서 2      ← 같은 동물인데 문서가 늘어난다
  animal.animals+0+102  → 문서 3
```

**즉 `upsert`로 설정했지만 실제로는 매 변경마다 새 문서가 생깁니다.**
찜 카운트가 올라갈 때마다(그때마다 `animals` 행이 UPDATE됨) 문서가 하나씩 쌓입니다.

한편 배치 경로는 `esId`를 문서 ID로 쓰므로, 매일 새벽 전체 삭제 후 정상 문서만 남깁니다.
**결과적으로 "새벽에는 깨끗하고 낮에 갈수록 중복이 쌓이는" 인덱스가 됩니다.**
검색 결과에 같은 동물이 여러 번 나타날 수 있습니다.

**고치는 방법**

```json
// (A) 문서 ID를 데이터에서 뽑는다
"key.ignore": "false",
// + Debezium이 PK를 메시지 키로 넣으므로 그 값이 _id 가 된다
// 단, 키가 {"id": 3} 구조체라서 ExtractField SMT 로 값만 꺼내야 한다

// (B) 두 경로 중 하나만 남긴다 — 더 단순하고 권장
//     CDC 실시간 동기화를 쓰기로 하면 배치 Step 2 를 제거
//     배치만 쓰기로 하면 animal-animals-connector + elasticsearch-sink 를 제거
```

**(B)가 현실적입니다.** 두 개를 유지하면 "어느 쪽이 맞는 값인가"를 계속 따져야 합니다.
`AnimalDocument`의 필드명이 snake_case인 것도 CDC 경로를 위한 설계입니다.

```java
/** 인덱스 필드명은 snake_case (DB 컬럼명과 동일) */
@Field(name = "apms_desertion_no", type = FieldType.Keyword)
private String apmsDesertionNo;
```

**CDC는 DB 컬럼명을 그대로 보내므로**, 배치가 쓰는 문서와 CDC가 쓰는 문서의 필드명을 맞추려고
snake_case를 쓴 것입니다. **두 경로를 동시에 유지하려는 노력의 흔적**이고,
그래서 `esId`(ES용 ID)와 `id`(MySQL PK)를 분리하는 복잡함까지 생겼습니다.
하나로 정리하면 이 복잡함도 함께 사라집니다.

### 4-5. ⚠️ 커넥터 설정이 서비스마다 다릅니다

| 항목 | user | community | store-v3 | payment-v5 | animal-animals |
|---|---|---|---|---|---|
| `value.converter` 명시 | ⭕ | ❌ (기본값) | ⭕ | ⭕ | ❌ |
| `value.converter.schemas.enable` | `false` | (기본 `true`) | `false` | `false` | (기본) |
| `event.id` 컬럼 | `event_id` | `event_id` | `id` | `id` | — |
| `expand.json.payload` | `true` | `true` | `true` | `true` | — |
| `snapshot.mode` | `when_needed` | (기본 `initial`) | `initial` | (기본) | `initial` |
| `additional.placement` | `event_type` | `type` | `event_type` | `event_type` | — |

**이 차이가 소비자 코드 모양을 결정합니다.**

```java
// community — schemas.enable 이 true → 한 겹 벗겨야 한다
Map<String, Object> debeziumMessage = objectMapper.readValue(payloadJson, Map.class);
Map<String, Object> payload = (Map<String, Object>) debeziumMessage.get("payload");

// animal (user 커넥터의 메시지) — schemas.enable 이 false → 바로 받는다
public void consumeFavoriteEvent(Map<String, Object> payload, Acknowledgment ack)

// store (payment 커넥터의 메시지) — 확신이 없어 4가지 경우를 모두 방어한다
if (root.has("payload") && root.get("payload").isTextual())      root = readTree(...asText());
else if (root.has("payload") && root.get("payload").isObject())  root = root.get("payload");
```

**소비자 코드의 복잡함이 커넥터 설정 불일치에서 나왔습니다.**
설정을 통일하면 세 소비자가 같은 모양이 됩니다.

```json
// 모든 Outbox 커넥터에 공통으로 명시할 것
"key.converter": "org.apache.kafka.connect.json.JsonConverter",
"key.converter.schemas.enable": "false",
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "false",
"transforms.outbox.table.expand.json.payload": "true"
```

`schemas.enable: false`가 실용적인 이유가 하나 더 있습니다.
스키마는 **데이터보다 클 수 있습니다.** 게시글 payload 1KB에 스키마 2KB가 붙으면
Kafka 저장 용량과 네트워크가 3배가 됩니다.

`snapshot.mode` 차이도 알아둘 만합니다.

```
initial       처음 시작할 때 테이블 전체를 스냅샷 → 기존 데이터 전부 이벤트로 발행
when_needed   필요할 때만 (오프셋이 유효하지 않을 때)
never         스냅샷 없이 지금부터의 변경만
```

**Outbox 테이블에 `initial`은 위험할 수 있습니다.** 커넥터를 재등록하면
outbox에 남아 있는 과거 이벤트 전부가 **다시 발행**됩니다.
소비자의 멱등성(`processed_events`)이 이걸 막아주지만,
[store와 payment는 `processed_events`가 없어](06-store-service.md#7-5-멱등성이-상태-검사에만-의존한다)
상태 검사에만 의존합니다. `outbox_events` 정리 배치(7일)가 그 위험을 줄여줍니다.

### 4-6. 🚨 커넥터 JSON에 DB 자격증명이 하드코딩되어 git에 있습니다

**8개 커넥터 파일 전부** 같습니다.

```json
"database.hostname": "db-server",
"database.user": "root",
"database.password": "root"
```

- **root 계정을 씁니다.** CDC에 필요한 권한은 `REPLICATION SLAVE`, `REPLICATION CLIENT`, `SELECT`뿐입니다.
  root는 과합니다.
- **비밀번호가 git에 커밋되어 있습니다.** [.gitignore](../.gitignore)는 `.env`와
  `**/application-{local,dev,prod,secret}.yml`을 꼼꼼히 제외하고 있는데, **커넥터 JSON만 빠졌습니다.**
  비밀값 관리 규율이 있는데 이 파일들이 예외가 된 것입니다.

**이미 해결 선례가 있습니다.** `deploy-monitoring.yml`이 `envsubst`로 prometheus.yml의
환경변수를 치환합니다(커밋 `df5f2c9`). **같은 방법을 커넥터에도 적용하면 됩니다.**

```json
// connectors/user-outbox-connector.json
"database.hostname": "${DB_HOST}",
"database.user": "${CDC_USER}",
"database.password": "${CDC_PASSWORD}"
```

```bash
# 등록 전에 치환
envsubst < user-outbox-connector.json > /tmp/user.json
curl -X PUT .../connectors/user-outbox-connector/config -d @/tmp/user.json
```

그리고 CDC 전용 계정을 만듭니다.

```sql
CREATE USER 'cdc'@'%' IDENTIFIED BY '...';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
```

⚠️ `infrastructure/prod/connectors/elasticsearch-sink-connector.json`에는
`http://<ELASTICSEARCH_HOST>:9200` 이라는 **치환되지 않는 플레이스홀더**가 남아 있습니다.
`envsubst`는 `${VAR}` 형식만 치환하므로 `<...>`는 그대로 남아 **커넥터 등록이 실패**합니다.
`${ELASTICSEARCH_HOST}` 형식으로 바꿔야 합니다.

---

## 5. Elasticsearch — nori 커스텀 이미지

[infrastructure/elasticsearch/Dockerfile](../infrastructure/elasticsearch/Dockerfile)

```dockerfile
FROM docker.elastic.co/elasticsearch/elasticsearch:7.17.0
RUN elasticsearch-plugin install analysis-nori
```

한국어 형태소 분석 플러그인을 설치한 이미지를 만들어 Docker Hub에 올려 씁니다
(`${DOCKER_USERNAME}/elasticsearch-nori:latest`).

**nori 분석기의 원리와 사용자 사전 설정은
[04-animal-service.md 4절](04-animal-service.md#4-elasticsearch--한국어-검색을-가능하게-하는-것들)에서
자세히 다뤘습니다.**

여기서는 인덱스가 두 가지 방식으로 만들어진다는 점만 정리합니다.

| 인덱스 | 만드는 방법 | 커스텀 분석기 | 사용자 사전 |
|---|---|---|---|
| `animals` | `mappings/animals-index-mapping.json` 을 스크립트로 적용 | `nori_analyzer` | ⭕ 품종 12개 |
| `store` | `mappings/store-index-mapping.json` | (별도 정의) | — |
| `posts` | **Spring Data가 자동 생성** | 내장 `nori` | ❌ |

`setup-index.sh`, `setup-index.bat`, `setup-store-index.bat` 로 매핑을 적용합니다.
**인덱스 매핑은 만든 뒤에 바꿀 수 없으므로**(필드 타입 변경 불가) 미리 정의하는 것이 맞습니다.

⚠️ **`posts` 인덱스만 자동 생성이라 사용자 사전·품사 필터가 적용되지 않습니다.**
[05-community-service.md 4-1](05-community-service.md#4-1-fieldanalyzer--nori--animal-service와-다른-방식)에서
지적한 내용이고, 매핑 파일을 하나 더 만들어 같은 방식으로 적용하면 해결됩니다.

⚠️ `"number_of_replicas": 0` — 복제본이 없습니다. 노드가 1대이므로 당연하지만,
**ES 노드가 죽으면 검색 인덱스가 사라집니다.** 다만 MySQL이 원본이므로 재색인으로 복구됩니다.
(animal은 배치, community는 `SyncScheduler`가 그 역할입니다.)

---

## 6. MySQL — 비용 절감을 명시한 단일 인스턴스

[infrastructure/mysql/init-sql/init.sh](../infrastructure/mysql/init-sql/init.sh)

```bash
# PawBridge 마이크로서비스 데이터베이스 초기화 스크립트
# 서비스별 논리적 DB 분리 (스키마 분리 전략)
# AWS 비용 절감을 위해 단일 MySQL 인스턴스에서 스키마로 분리

CREATE DATABASE IF NOT EXISTS pawbridge_user      CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS pawbridge_animal    ...
CREATE DATABASE IF NOT EXISTS pawbridge_community ...
CREATE DATABASE IF NOT EXISTS pawbridge_store     ...
CREATE DATABASE IF NOT EXISTS pawbridge_payment   ...
```

**주석에 트레이드오프를 명시한 것이 좋습니다.** "MSA인데 DB를 하나 쓴다"는 지적을 받을 지점을
스스로 알고 이유를 남겼습니다.

**얻은 것과 잃은 것**

```
얻은 것
· RDS 인스턴스 1개 비용 (5개면 5배)
· 관리 대상 1개
· 스키마는 분리되어 있어 "다른 서비스 테이블을 조회"하는 일은 코드상 막힌다

잃은 것
· 장애 격리 실패 — node-2가 죽으면 5개 서비스 전부 정지
· 자원 격리 실패 — animal 배치가 DB를 점유하면 결제도 느려진다
· 스키마 변경 시 다른 서비스에 영향 (락, 부하)
```

**`utf8mb4` 지정이 중요합니다.** `utf8`(=`utf8mb3`)은 3바이트까지만 저장해
**이모지가 들어가면 오류가 납니다.** 커뮤니티 게시글에 이모지는 반드시 들어옵니다.
`utf8mb4`는 4바이트를 지원해 이모지가 저장됩니다.

`utf8mb4_unicode_ci`의 `ci`는 case-insensitive(대소문자 구분 없음)입니다.
`'Dog'`와 `'dog'`가 같게 취급됩니다. 검색·중복 판정에 편리합니다.

⚠️ **`ddl-auto: update`** 가 모든 서비스에 설정되어 있습니다(config repo).

```yaml
spring:
  jpa:
    hibernate:
      ddl-auto: update  # 개발환경
```

주석에 "개발환경"이라 적혀 있지만 **실제 배포 프로필(dev)이 이 값을 씁니다.**

```
update 의 위험
· 컬럼 추가는 하지만 삭제·타입변경은 하지 않는다 → 코드와 DB가 조용히 어긋난다
· 애플리케이션이 뜰 때마다 DDL을 실행한다 → 여러 인스턴스가 동시에 뜨면 충돌 가능
· 인덱스·제약 이름이 Hibernate 마음대로 붙는다
· 무엇보다 "언제 무엇이 바뀌었는지" 기록이 없다
```

**Flyway나 Liquibase로 마이그레이션을 관리하는 것이 정석입니다.**
`ddl-auto: validate`로 바꾸고 스키마 변경은 마이그레이션 스크립트로 하면,
변경 이력이 git에 남고 롤백도 가능해집니다.

---

## 7. 모니터링 — Prometheus + Grafana + Zipkin

### 7-1. 세 도구의 역할이 다릅니다

```
Prometheus  "숫자를 시간에 따라 모은다"      → 요청 수, 응답시간 분포, JVM 힙, CPU
Grafana     "그 숫자를 그래프로 보여준다"     → 대시보드, 알림
Zipkin      "요청 하나가 지나간 길을 보여준다" → 분산 추적
```

**Zipkin이 MSA에서 특히 중요합니다.**

```
마이페이지 요청이 3초 걸렸다. 어디가 느렸나?

로그만 보면:  게이트웨이 로그, user 로그, animal 로그, store 로그를 각각 열어
              시간을 대조해야 한다. 요청이 섞여 있어 어느 게 그 요청인지도 모른다.

Zipkin:  하나의 화면에
         gateway     ▉ 5ms
         user-service  ▉▉▉ 120ms
           └ animal-service  ▉ 30ms
           └ store-service   ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 2800ms   ← 여기다
```

이걸 가능하게 하는 것이 **Trace ID 전파**입니다.

```gradle
implementation 'io.micrometer:micrometer-tracing-bridge-brave'
implementation 'io.zipkin.reporter2:zipkin-reporter-brave'
```

이 두 의존성이 **모든 서비스에 들어 있습니다.** Micrometer Tracing이
요청마다 Trace ID를 만들고, **Feign 호출·Kafka 발행 시 헤더에 자동으로 실어 보냅니다.**
받는 쪽은 그 ID를 이어받아 같은 추적에 기록합니다.

```yaml
# config repo: application-dev.yml
management:
  zipkin:
    tracing:
      endpoint: http://${NODE1_PRIVATE_IP}:9411/api/v2/spans
  tracing:
    sampling:
      probability: 1.0        # ★ 100% 수집
```

**`probability: 1.0`은 모든 요청을 추적합니다.** 개발·소규모에서는 맞습니다.
트래픽이 늘면 Zipkin 저장 부하가 커지므로 보통 0.1(10%)로 낮춥니다.

⚠️ `STORAGE_TYPE=mem` — Zipkin이 **메모리에만** 저장합니다.
컨테이너를 재시작하면 추적 기록이 전부 사라집니다. 실시간 디버깅에는 충분하지만
"어제 그 장애"를 조사할 수는 없습니다.

### 7-2. Prometheus의 pull 방식

```yaml
- job_name: 'user-service'
  metrics_path: '/actuator/prometheus'
  static_configs:
    - targets: ['${NODE4_PRIVATE_IP}:8081']
```

**Prometheus는 서비스가 보내주는 게 아니라 직접 가져갑니다(pull).**
각 서비스는 `/actuator/prometheus` 경로에 현재 숫자를 텍스트로 노출하고,
Prometheus가 주기적으로 그 URL을 긁습니다.

```gradle
implementation 'io.micrometer:micrometer-registry-prometheus'
```
```yaml
# config repo: application.yml (전 서비스 공통)
management:
  endpoints:
    web:
      exposure:
        include: health,info,refresh,prometheus     # ★ prometheus 엔드포인트 노출
  metrics:
    tags:
      application: ${spring.application.name}       # ★ 모든 메트릭에 서비스 이름 태그
```

**`tags.application`이 중요합니다.** 이게 없으면 Grafana에서
`jvm_memory_used_bytes`가 어느 서비스 것인지 구분할 수 없습니다.
태그가 있으면 `jvm_memory_used_bytes{application="user-service"}`로 필터할 수 있습니다.

### 7-3. dev용과 prod용 prometheus.yml이 다릅니다

| | `infrastructure/monitoring/prometheus/prometheus.yml` | `infrastructure/prod/prometheus/prometheus.yml` |
|---|---|---|
| 용도 | 로컬 개발 | EC2 배포 |
| 대상 주소 | `host.docker.internal:8081` | `${NODE4_PRIVATE_IP}:8081` |
| scrape 주기 | 5초 | 15초 |
| 배포 방법 | 직접 실행 | `envsubst`로 치환 후 scp |

**`host.docker.internal`은 Docker Desktop(Mac/Windows) 전용입니다.**
리눅스 EC2에서는 기본적으로 해석되지 않습니다. 그래서 운영용을 따로 둔 것이 맞습니다.

**`envsubst` 치환 단계**([.github/workflows/deploy-monitoring.yml](../.github/workflows/deploy-monitoring.yml))

```yaml
- name: Replace environment variables in prometheus.yml
  run: |
    export NODE2_PRIVATE_IP=${{ secrets.NODE2_PRIVATE_IP }}
    export NODE3_PRIVATE_IP=${{ secrets.NODE3_PRIVATE_IP }}
    export NODE4_PRIVATE_IP=${{ secrets.NODE4_PRIVATE_IP }}
    export NODE5_PRIVATE_IP=${{ secrets.NODE5_PRIVATE_IP }}
    envsubst < infrastructure/prod/prometheus/prometheus.yml > prometheus.yml.tmp
    mv prometheus.yml.tmp infrastructure/prod/prometheus/prometheus.yml
```

**사설 IP를 git에 넣지 않으면서 설정 파일에 넣는 방법**입니다.
GitHub Secrets에 보관하고 배포 시점에 치환합니다. 좋은 패턴이고,
[4-6](#4-6--커넥터-json에-db-자격증명이-하드코딩되어-git에-있습니다)의 커넥터 문제도
바로 이 방법으로 해결할 수 있습니다.

### 7-4. ★ 포트를 노출한 진짜 이유

[02-api-gateway.md 6-1](02-api-gateway.md#6-1-게이트웨이-우회--헤더를-무조건-믿는다)에서
"서비스 포트가 열려 있다"고 지적했습니다. **그 이유가 여기 있습니다.**

```
Prometheus 는 node-1 에 있고, user-service 는 node-4 에 있다
→ node-1 의 Prometheus 가 node-4 의 8081 포트를 긁어야 한다
→ 그래서 docker-compose 에서 ports: ["8081:8081"] 로 노출했다
```

**즉 포트 노출은 실수가 아니라 모니터링을 위한 필요였습니다.**
따라서 "`ports:`를 지워라"는 단순한 처방은 맞지 않습니다. 대안은 이렇습니다.

```yaml
# (A) 사설 IP에만 바인딩한다 — 가장 간단
ports:
  - "10.0.4.11:8081:8081"      # 공인 IP로는 접근 불가
```

```yaml
# (B) actuator만 별도 포트로 분리한다 — 가장 정확
management:
  server:
    port: 9081                  # 메트릭은 9081, 비즈니스 API는 8081
```
```yaml
ports:
  - "9081:9081"                 # 관리 포트만 노출, 8081은 닫는다
```

**(B)가 정석입니다.** Spring Boot는 `management.server.port`로 actuator를 별도 포트에 띄울 수
있습니다. 그러면 비즈니스 API 포트를 완전히 닫으면서 모니터링은 유지됩니다.

여기에 보안그룹으로 "node-1에서 온 요청만 그 포트 허용"을 걸면 이중 방어가 됩니다.

### 7-5. Grafana 프로비저닝

```yaml
volumes:
  - ./grafana/provisioning/datasources:/etc/grafana/provisioning/datasources
  - ./grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards
```

**대시보드와 데이터소스를 파일로 관리합니다.** Grafana UI에서 손으로 만들면
컨테이너를 재생성할 때 사라지거나, 다른 환경에 옮길 수 없습니다.
프로비저닝 파일로 두면 **git에 남고 자동 복원됩니다.**

⚠️ `GF_SECURITY_ADMIN_PASSWORD=admin` — 기본 비밀번호가 그대로입니다.
`grafana:3000`이 열려 있으면 누구나 대시보드에 접근합니다.
메트릭에는 트래픽 패턴·에러율 같은 정보가 담기므로 공개하면 안 됩니다.

---

## 8. CI/CD — 서비스별 독립 배포

### 8-1. 워크플로우 10개

```
.github/workflows/
├── deploy-gateway.yml        → node-1
├── deploy-discovery.yml      → node-1
├── deploy-config.yml         → node-2
├── deploy-animal.yml         → node-3
├── deploy-user.yml           → node-4
├── deploy-community.yml      → node-4
├── deploy-store.yml          → node-4
├── deploy-payment.yml        → node-4
├── deploy-elastic-connect.yml→ node-5
└── deploy-monitoring.yml     → node-1
```

### 8-2. 경로 필터 — 바뀐 것만 배포한다

```yaml
on:
  push:
    branches: [ "dev" ]
    paths:
      - 'user-service/**'
      - 'deployment/docker-compose-node-4.yml'
      - '.github/workflows/deploy-user.yml'
```

**모노레포에서 반드시 필요한 설정입니다.**
없으면 커밋 하나에 9개 서비스가 전부 빌드·배포됩니다.
`user-service/` 아래 파일이 바뀔 때만 user-service를 배포합니다.

세 가지를 감시하는 것도 정확합니다: **서비스 코드**, **그 서비스가 속한 compose 파일**,
**워크플로우 자체**.

### 8-3. 배포 절차

```yaml
- uses: actions/setup-java@v3            # ① JDK 17
  with: { java-version: '17', distribution: 'temurin' }

- name: Build User Service                # ② 빌드 (테스트 건너뜀)
  run: |
    cd user-service
    chmod +x gradlew
    ./gradlew clean build -x test

- uses: docker/login-action@v2             # ③ Docker Hub 로그인
- run: |                                   # ④ 이미지 빌드 & 푸시
    cd user-service
    docker build -t ${{ secrets.DOCKER_USERNAME }}/user-service:latest .
    docker push ${{ secrets.DOCKER_USERNAME }}/user-service:latest

- uses: appleboy/scp-action@v0.1.7         # ⑤ compose 파일 전송
  with: { host: ${{ secrets.EC2_HOST_4 }}, source: "deployment/docker-compose-node-4.yml" }

- uses: appleboy/ssh-action@v1.0.0         # ⑥ 원격 실행
  with:
    script: |
      cd ~/deploy
      mv docker-compose-node-4.yml docker-compose.yml
      echo "DOCKER_USERNAME=..." > .env             # ⑦ .env 를 매번 새로 생성
      echo "JWT_SECRET=${{ secrets.JWT_SECRET }}" >> .env
      ... (수십 줄)
      docker compose pull user-service
      docker compose up -d user-service
```

**`cd user-service && ./gradlew`가 핵심입니다.**
루트에 Gradle 빌드가 없는 **독립 프로젝트 모노레포**이므로 각 폴더로 들어가 빌드합니다.
[00-overview.md](00-overview.md#모노레포인데-하나의-빌드가-아니다)에서 다룬 구조가 여기에 반영되어 있습니다.

### 8-4. ⚠️ 배포 파이프라인의 문제 세 가지

**① 테스트를 건너뜁니다.**

```bash
./gradlew clean build -x test
```

`-x test`가 테스트를 제외합니다. 사실 **테스트가 사실상 없으므로**(9개 파일 전부 `contextLoads` 스켈레톤)
지금은 차이가 없습니다. 하지만 테스트를 작성하기 시작하면 **이 플래그를 지우는 것이 첫 단계**입니다.
테스트가 CI에서 돌지 않으면 아무도 실행하지 않게 됩니다.

**② `:latest` 태그만 씁니다.**

```bash
docker build -t ${{ secrets.DOCKER_USERNAME }}/user-service:latest .
```

```
문제
· 롤백이 불가능하다 — "어제 버전"이 존재하지 않는다
· 지금 도는 컨테이너가 어느 커밋인지 알 수 없다
· 여러 노드가 서로 다른 latest 를 갖고 있을 수 있다
```

```bash
# 개선: 커밋 SHA를 태그로 함께 붙인다
docker build -t $USER/user-service:${{ github.sha }} -t $USER/user-service:latest .
docker push $USER/user-service:${{ github.sha }}
docker push $USER/user-service:latest
# 롤백:  docker compose up -d 하기 전에 이미지 태그를 이전 SHA로 바꾼다
```

**③ `.env`를 매번 `echo`로 다시 만듭니다.**

`deploy-user.yml` 한 파일에 `echo "..." >> .env`가 **30줄 넘게** 이어집니다.
그리고 **node-4의 4개 서비스가 같은 `.env`를 공유**하므로,
user만 배포해도 store·payment·community의 환경변수를 **전부 다시 써야 합니다.**

```
문제
· deploy-user.yml, deploy-store.yml, deploy-payment.yml, deploy-community.yml 에
  같은 .env 생성 블록이 4번 복사되어 있다
· 하나에 변수를 추가하면 나머지 3개도 고쳐야 한다 → 잊으면 그 서비스만 환경변수가 사라진다
· 실제로 deploy-user.yml 안에 NODE{1..5}_PRIVATE_IP 가 두 번 중복 기록되어 있다
```

그리고 **주석에 실수의 흔적**이 남아 있습니다.

```yaml
# 3. 배포 (Node-2로 전송)
# 3. 배포 (Node-4로 전송)      ← 위 줄을 고치지 않고 아래에 새로 적었다
```

```yaml
# Email (변수명 변환!)
echo "GOOGLE_EMAIL=${{ secrets.MAIL_USERNAME }}" >> .env
```

`MAIL_USERNAME` 시크릿을 `GOOGLE_EMAIL` 환경변수로 바꿔 넣습니다.
**시크릿 이름과 코드가 기대하는 변수 이름이 어긋나서** 생긴 변환이고,
`(변수명 변환!)` 이라는 느낌표가 그 혼란을 보여줍니다.

**개선 방향**

```yaml
# (A) .env 생성을 재사용 가능한 워크플로우로 분리
# .github/workflows/_make-env-node4.yml 을 만들고 workflow_call 로 호출
jobs:
  deploy:
    uses: ./.github/workflows/_make-env-node4.yml
    secrets: inherit

# (B) 더 나은 방향: .env 를 아예 없앤다
#     이미 Config Server 가 있으므로, 컨테이너에는 최소한의 부트스트랩 변수만 준다
#       CONFIG_SERVER_HOST, CONFIG_USERNAME, CONFIG_PASSWORD, SPRING_PROFILES_ACTIVE
#     나머지는 모두 config repo 에서 받는다 (이미 {cipher} 로 암호화되어 있다)
```

**(B)가 근본 해법입니다.** 지금은 **설정이 두 곳(`.env`와 config repo)에 이중으로** 존재해서
어느 값이 실제로 쓰이는지 추적이 어렵습니다.
실제로 [07-payment-service.md](07-payment-service.md#-그런데-이-검증-호출이-404를-맞습니다)에서 본
`STORE_SERVICE_URL` / `service.store.url` 이름 불일치가 그런 혼란의 예입니다.

### 8-5. 배포 시 무중단이 아닙니다

```bash
docker compose up -d user-service
```

기존 컨테이너를 멈추고 새 컨테이너를 띄웁니다. 그 사이 **수 초~수십 초 동안 요청이 실패**합니다.
게이트웨이는 Eureka에서 인스턴스를 받아오는데, 죽은 인스턴스가 목록에서 빠지는 데도 시간이 걸립니다
([01-discovery-config.md](01-discovery-config.md#설정에서-눈여겨볼-두-줄)의 eviction 3초 설정이
그 시간을 줄이려는 조치입니다).

**무중단으로 가려면** 인스턴스를 2개 이상 띄우고 하나씩 교체해야 합니다(rolling update).
지금 구조에서는 같은 노드에 포트를 달리해 2개를 띄우고 Eureka가 로드밸런싱하게 하면
간단한 형태로 구현할 수 있습니다.

---

## 9. `integration-service` — 죽은 모듈

```
integration-service/src/
├── main/java/.../IntegrationServiceApplication.java   (빈 애플리케이션)
├── main/resources/application.yml
└── test/java/.../IntegrationServiceApplicationTests.java (빈 테스트)
```

**자바 파일이 2개뿐이고 비즈니스 코드가 없습니다.**

- `.github/workflows/`에 배포 워크플로우가 **없습니다.**
- `deployment/docker-compose-node-*.yml`에 **등장하지 않습니다.**
- 다른 서비스가 참조하지 **않습니다.**
- `Dockerfile`도 없습니다.

`build.gradle`에는 `spring-boot-starter-batch`, `data-jpa`, `openfeign`이 선언되어 있어
**"여러 서비스의 데이터를 모으는 통합 배치"를 시도하다 멈춘 흔적**으로 보입니다.
(그 역할은 결국 animal-service의 배치와 각 서비스의 스케줄러로 흩어졌습니다.)

**지우거나, 남길 이유를 README에 적어두는 것이 좋습니다.**
지금 상태로는 새로 합류한 사람이 "이건 뭐지? 왜 안 도는 거지?"에 시간을 씁니다.

---

## 10. 이 문서에서 배울 개념 정리

| 개념 | 한 줄 설명 | 위치 |
|---|---|---|
| **TLS 종료** | 인증서·암복호화를 진입점 한 곳에 모은다 | nginx |
| **`X-Forwarded-*`** | 없으면 IP 로그와 HTTPS 리다이렉트가 깨진다 | nginx |
| **Kafka 리스너 이중화** | `advertised.listeners`가 접속 경로별로 달라야 한다 | 3-1 ★ |
| **토픽 자동 생성 끄기** | 오타 토픽이 조용히 생기는 것을 막는다 | 3-2 |
| **`cleanup.policy=compact`** | 같은 키의 마지막 값만 남긴다 (설정 저장용) | 3-2 |
| **`cub kafka-ready`** | `depends_on`은 "준비됨"을 보장하지 않는다 | 3-2 |
| **멱등한 부트스트랩** | 409를 정상으로 처리해야 재배포가 안전하다 | 3-3 |
| **CDC / binlog** | Debezium이 복제 슬레이브로 위장해 변경을 읽는다 | 4-1 |
| **`server.id` 고유성** | 겹치면 커넥터가 서로를 끊는다 | 4-1 |
| **EventRouter SMT** | Outbox 행을 순수 도메인 이벤트로 변환 | 4-2 ★ |
| **메시지 키 = 순서 보장 단위** | `table.field.event.key`가 파티션을 결정 | 4-2 |
| **`route.by.field`** | 고정 토픽이면 모든 이벤트가 섞인다 | 4-2 |
| **`key.ignore: true`** | 문서 ID가 오프셋 기반 → upsert가 아니라 중복 생성 | 4-4 ★ |
| **`schemas.enable`** | 소비자 코드 모양을 결정한다. 용량도 3배 차이 | 4-5 |
| **`snapshot.mode`** | 커넥터 재등록 시 과거 이벤트 재발행 여부 | 4-5 |
| **`envsubst` 치환** | 사설 IP·비밀값을 git에 넣지 않고 설정 파일에 넣는다 | 7-3 ★ |
| **`utf8mb4`** | 이모지를 저장하려면 4바이트가 필요하다 | 6절 |
| **`ddl-auto: update`의 위험** | 삭제·타입변경을 하지 않아 코드와 DB가 어긋난다 | 6절 |
| **Prometheus pull 방식** | 서비스가 노출하고 Prometheus가 긁어간다 | 7-2 |
| **`metrics.tags.application`** | 없으면 어느 서비스 메트릭인지 구분 못 한다 | 7-2 |
| **분산 추적(Trace ID 전파)** | 요청 하나가 지나간 길을 한 화면에서 본다 | 7-1 ★ |
| **`management.server.port`** | actuator를 별도 포트로 분리해 API 포트를 닫는다 | 7-4 ★ |
| **Grafana 프로비저닝** | 대시보드를 파일로 관리해 재현 가능하게 | 7-5 |
| **경로 필터 CI** | 모노레포에서 바뀐 서비스만 배포 | 8-2 |
| **이미지 태그와 롤백** | `:latest`만 쓰면 되돌릴 수 없다 | 8-4 ★ |

---

## 11. 개선 우선순위

| 순위 | 항목 | 심각도 | 근거 |
|---|---|---|---|
| 1 | 🚨 커넥터 JSON의 `root/root` 자격증명 제거 + CDC 전용 계정 | **높음** | 비밀번호가 git에 커밋됨 (4-6) |
| 2 | 🚨 `animals` 인덱스의 중복 문서 — 동기화 경로를 하나로 | **높음** | `key.ignore: true`로 변경마다 문서 증가 (4-4) |
| 3 | actuator를 별도 포트로 분리 (또는 사설 IP 바인딩) | **높음** | 서비스 포트가 열려 있어야 하는 이유를 제거 (7-4) |
| 4 | 이미지 태그에 커밋 SHA 추가 | **높음** | 롤백 불가 (8-4) |
| 5 | store outbox 토픽을 aggregateType별로 분리 | 중간 | 주문 이벤트와 상품 문서가 한 인덱스에 섞임 (4-2) |
| 6 | 커넥터 `schemas.enable`·`event.id` 설정 통일 | 중간 | 소비자 코드가 서비스마다 다름 (4-5) |
| 7 | `ddl-auto: update` → Flyway 마이그레이션 | 중간 | 스키마 변경 이력이 없다 (6절) |
| 8 | `.env` 생성을 재사용 워크플로우로 (또는 Config Server로 통합) | 중간 | 4개 워크플로우에 중복, 누락 위험 (8-4) |
| 9 | `connector-init`을 POST → PUT으로 | 중간 | 설정 변경이 반영되지 않아 `-v3`, `-v5`가 생김 (3-3) |
| 10 | prod ES 싱크의 `<ELASTICSEARCH_HOST>` 플레이스홀더 수정 | 중간 | `envsubst`가 치환하지 못해 등록 실패 (4-6) |
| 11 | `posts` 인덱스에 매핑 파일 적용 | 중간 | 사용자 사전·품사 필터 미적용 (5절) |
| 12 | Grafana 기본 비밀번호 변경 | 중간 | `admin/admin` (7-5) |
| 13 | ES 싱크 플러그인 버전 고정 (`:latest` 제거) | 낮음 | 재빌드 시 동작이 달라질 수 있다 (3-4) |
| 14 | `-x test` 제거 (테스트 작성 후) | 낮음 | CI에서 안 돌면 아무도 실행하지 않는다 (8-4) |
| 15 | Zipkin `STORAGE_TYPE`을 영속 저장소로 | 낮음 | 재시작 시 추적 기록 소실 (7-1) |
| 16 | `integration-service` 제거 또는 문서화 | 낮음 | 죽은 모듈 (9절) |
| 17 | Kafka 파티션 수 계획 (동시성 수정 후) | 낮음 | 지금 늘리면 갱신 유실이 발생한다 (3-2) |

---

**돌아가기** → [00-overview.md](00-overview.md) — 전체 지도와 문서 색인
