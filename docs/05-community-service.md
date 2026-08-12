# 05. community-service — 게시글·댓글, Soft Delete, 그리고 세 번째 Outbox 구현

> 자바 파일 55개로 비교적 작습니다. 하지만 **같은 Outbox 패턴의 세 번째 구현**이라
> user-service·animal-service와 비교하면 "같은 문제를 세 사람이 어떻게 다르게 풀었나"가 보입니다.
> 그리고 **N+1 HTTP 호출**이라는, MSA에서 가장 흔한 성능 함정이 여기 있습니다.

```
community-service/src/main/java/com/pawbridge/communityservice/
├── domain/
│   ├── entity/      Post, Comment, BoardType, OutboxEvent, ProcessedEvent, StringListConverter
│   └── repository/
├── dto/             request(4), response(2)
├── controller/      PostController, CommentController, SearchController, AdminPostController
├── service/         Post/Comment/Search/S3/Outbox
├── elasticsearch/   PostDocument
├── kafka/           PostEventConsumer, PostEventHandler   ★ Outbox의 소비자 쪽
├── client/          UserServiceClient (Feign — 닉네임 조회)
├── scheduler/       CleanupScheduler, SyncScheduler        ★ 유실 복구
├── config/          Elasticsearch, Jpa, KafkaConsumer, Scheduler
└── util/            ResponseDTO, CustomResponseUtil        (user-service에서 전파된 규약)
```

---

## 1. 도메인 모델 — 단순함이 의도된 설계

### 게시판 종류를 enum으로

[domain/entity/BoardType.java](../community-service/src/main/java/com/pawbridge/communityservice/domain/entity/BoardType.java)

```java
public enum BoardType {
    MISSING,        // 실종
    PROTECTION,     // 보호
    REPORT,         // 제보
    ADOPTION,       // 입양후기
    COMMUNICATION   // 소통
}
```

게시판을 별도 테이블(`boards`)로 만들지 않고 enum으로 했습니다. **게시판이 사용자가 추가하는 데이터가
아니라 서비스가 정하는 고정 목록**이기 때문에 옳은 선택입니다. 테이블로 만들면 조인이 늘고,
"게시판 목록 조회 API"가 필요해지고, 코드에서 `boardId == 3`처럼 의미 없는 숫자를 쓰게 됩니다.

유기동물 플랫폼의 성격이 게시판 이름에 드러나는 것도 좋습니다. 일반 커뮤니티의 "자유게시판/질문게시판"이
아니라 **실종·보호·제보·입양후기**입니다. 도메인을 반영한 분류입니다.

### 댓글에 대댓글이 없다 — 명시된 선택

```java
/**
 * Comment 엔티티: 게시글 댓글
 * - 대댓글 없음 (parent_id 없음)
 * - Soft delete 지원 (deleted_at)
 */
```

대댓글(계층 댓글)은 구현 비용이 큽니다. `parentId`를 넣으면 조회 시 트리를 만들어야 하고,
정렬 규칙(부모 순 → 자식 순)이 복잡해지고, 부모가 삭제됐을 때 자식을 어떻게 할지 정해야 합니다.
**"안 만든다"를 주석으로 명시한 것은 좋은 습관입니다.** 나중에 보는 사람이 "빠뜨렸나?" 하고 헷갈리지 않습니다.

### Comment가 Post를 FK로 참조하지 않는다

```java
public class Comment {
    @Column(nullable = false, name = "post_id")
    private Long postId;          // ← @ManyToOne Post 가 아니라 그냥 Long

    @Column(nullable = false, name = "author_id")
    private Long authorId;        // ← 다른 서비스의 사용자 ID
}
```

`authorId`가 `Long`인 것은 **당연합니다.** `users` 테이블은 다른 서비스의 다른 DB에 있으므로
JPA 연관관계를 맺을 수 없습니다. **MSA에서 서비스 경계를 넘는 관계는 ID로만 표현합니다.**

`postId`도 같은 DB에 있는데 `Long`으로 뒀습니다. 이건 선택의 문제입니다.

```
@ManyToOne Post post   →  DB가 무결성을 보장. post.getComments() 편리. 단 LAZY 관리 필요.
Long postId            →  단순. 조회가 명시적. 단 존재하지 않는 postId도 저장 가능.
```

댓글 서비스에서 항상 "특정 게시글의 댓글 목록"만 조회하고 반대 방향을 쓰지 않는다면
`Long`이 더 단순합니다. 다만 **게시글이 없는 댓글이 생기는 것을 DB가 막아주지 않으므로**
애플리케이션이 항상 확인해야 합니다.

### Soft Delete — 지우지 않고 지운 것으로 표시한다

```java
@Column(name = "deleted_at")
private LocalDateTime deletedAt;

public void delete() {
    this.deletedAt = LocalDateTime.now();
}
```

조회는 항상 이 조건을 붙입니다.

```java
postRepository.findByPostIdAndDeletedAtIsNull(postId)
postRepository.findByDeletedAtIsNullOrderByCreatedAtDesc()
```

**왜 진짜로 지우지 않나**

| 이유 | 설명 |
|---|---|
| 복구 가능 | 사용자가 실수로 지웠다고 하면 되돌릴 수 있다 |
| 참조 무결성 | 게시글을 지워도 그 글의 댓글·신고 기록이 고아가 되지 않는다 |
| 감사·분쟁 | "이런 글을 올렸다"는 기록이 신고 처리에 필요하다 |
| 통계 | 삭제된 글도 활동량 집계에는 포함될 수 있다 |

**대가**: 모든 조회에 `deletedAtIsNull`을 붙여야 하고, **한 번이라도 빠뜨리면 삭제된 글이 노출됩니다.**
Hibernate의 `@Where(clause = "deleted_at is null")`이나 `@SQLRestriction`을 엔티티에 붙이면
조건이 자동으로 붙어 실수를 막을 수 있습니다.

⚠️ **주의: 여기엔 Soft Delete와 어긋나는 부분이 있습니다.**

```java
// PostServiceImpl.deletePost()
List<String> imageUrls = post.getImageUrls();
if (imageUrls != null && !imageUrls.isEmpty()) {
    imageUrls.forEach(s3Service::deleteFile);      // ← S3 파일은 진짜로 지운다
}
post.delete();                                      // ← DB는 표시만
```

**DB는 복구 가능한데 이미지는 영구 삭제됩니다.** 게시글을 되살려도 사진이 전부 깨집니다.
Soft Delete를 도입한 이유(복구 가능성)가 절반만 지켜진 상태입니다.

```java
// 개선: 파일 삭제는 별도 정리 배치로 미룬다
// 1) deletePost에서는 S3를 건드리지 않는다
// 2) "30일 넘게 deleted_at이 설정된 글"의 파일만 지우는 스케줄러를 만든다
//    → 그 사이에는 복구가 가능하고, 스토리지도 무한정 늘지 않는다
```

### `StringListConverter` — List를 JSON 컬럼에 담기

[domain/entity/StringListConverter.java](../community-service/src/main/java/com/pawbridge/communityservice/domain/entity/StringListConverter.java)

```java
@Converter
public class StringListConverter implements AttributeConverter<List<String>, String> {
    @Override
    public String convertToDatabaseColumn(List<String> attribute) {
        if (attribute == null || attribute.isEmpty()) return "[]";
        return objectMapper.writeValueAsString(attribute);
    }
    @Override
    public List<String> convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.isEmpty()) return new ArrayList<>();
        return objectMapper.readValue(dbData, new TypeReference<List<String>>() {});
    }
}
```
```java
// Post 엔티티
@Convert(converter = StringListConverter.class)
@Column(name = "image_urls", columnDefinition = "JSON")
private List<String> imageUrls = new ArrayList<>();
```

**대안과 비교하면 이 선택의 이유가 보입니다.**

```
① 별도 테이블 post_images (정석)
   장점: 정규화, 순서 컬럼 관리, 개별 조회 가능
   단점: 테이블 1개 + JOIN, 이미지만 쓰는데 과함

② @ElementCollection
   장점: JPA 표준
   단점: 사실상 ①과 같은 테이블이 생기고, 수정 시 전체 DELETE+INSERT

③ JSON 컬럼 + Converter  ← 선택
   장점: 게시글 1행에 다 들어감. 조회가 곧 게시글 조회.
   단점: SQL로 "특정 이미지를 쓰는 글 찾기"가 어렵다
```

이미지 URL은 **게시글과 생명주기가 완전히 같고, 개별로 조회할 일이 없는** 값입니다.
그런 데이터는 JSON 컬럼이 실용적입니다. `null`과 빈 리스트를 `"[]"`와 `new ArrayList<>()`로
정규화해 **NPE를 원천 차단**한 것도 꼼꼼합니다.

⚠️ `AttributeConverter`는 **JPA가 인스턴스를 만들기 때문에** 스프링 빈을 주입받을 수 없습니다.
그래서 `static ObjectMapper`를 씁니다. 올바른 대응입니다.
(Hibernate 6부터는 `@JdbcTypeCode(SqlTypes.JSON)`으로 컨버터 없이도 가능합니다.)

### 부분 수정(Partial Update) 패턴

```java
public void update(String title, String content, List<String> imageUrls) {
    if (title != null)     this.title = title;
    if (content != null)   this.content = content;
    if (imageUrls != null) this.imageUrls = imageUrls;
}
```

`null`이면 기존 값을 유지합니다. PATCH 요청을 지원하기 위한 흔한 방식입니다.

⚠️ **한계**: **값을 지울 방법이 없습니다.** "이미지를 전부 삭제하고 싶다"를 표현할 수 없습니다.
`imageUrls = []`(빈 배열)을 보내도 서비스 쪽에서 걸러집니다.

```java
// PostServiceImpl.updatePost()
if (images != null && images.length > 0) {     // ← 빈 배열은 "변경 없음"으로 처리됨
    ...
}
```

"수정 안 함"과 "비우기"를 구분하려면 별도 플래그(`removeImages: true`)나
`Optional<List<String>>`을 쓰는 방법이 있습니다.

---

## 2. Outbox 패턴 — 세 번째 구현

### 2-1. 구현은 올바릅니다

[service/OutboxServiceImpl.java](../community-service/src/main/java/com/pawbridge/communityservice/service/OutboxServiceImpl.java)

```java
@Override
@Transactional                    // ★ 기본 전파(REQUIRED) → 부모 트랜잭션에 합류
public String saveEvent(String aggregateType, String aggregateId, String eventType, Object payload) {
    String eventId = UUID.randomUUID().toString();
    String payloadJson = objectMapper.writeValueAsString(payload);   // ★ 주입받은 것 사용
    OutboxEvent event = OutboxEvent.builder()...build();
    outboxEventRepository.save(event);
    return eventId;
}
```

[03-user-service.md에서 지적한 두 문제](03-user-service.md#8-5--여기에-결함이-있습니다--requires_new가-원자성을-깬다)가
**여기서는 둘 다 없습니다.**

| | user-service | animal-service | **community-service** |
|---|---|---|---|
| 트랜잭션 전파 | `REQUIRES_NEW` ❌ | `@Transactional` ✅ | `@Transactional` ✅ |
| ObjectMapper | 매번 `new` ❌ | — | 주입받은 것 사용 ✅ |
| eventId 반환 | void | void | `String` 반환 ✅ |

`eventId`를 반환하는 것도 작지만 유용합니다. 호출자가 로그에 남기거나 추적에 쓸 수 있습니다.

### 2-2. Outbox 엔티티의 주석이 설계 의도를 잘 설명한다

[domain/entity/OutboxEvent.java](../community-service/src/main/java/com/pawbridge/communityservice/domain/entity/OutboxEvent.java)

```java
/**
 * Debezium 방식 특징:
 * - status, publishedAt, retryCount 필드 없음 (Polling 방식과 차이)
 * - Debezium이 binlog에서 INSERT를 감지하여 자동으로 Kafka 발행
 */
```

**이 주석이 중요한 개념을 설명합니다.** Outbox 패턴을 구현하는 방법은 두 가지입니다.

```
① Polling Publisher (직접 폴링)
   outbox_events 테이블에 status 컬럼을 두고
   @Scheduled 로 "status=PENDING 인 것을 조회 → Kafka 발행 → status=SENT 로 변경"

   필요한 컬럼:  status, published_at, retry_count
   장점: Debezium 등 추가 인프라가 없어도 된다
   단점: 폴링 지연(주기만큼 늦음), DB 부하, 발행과 상태 변경 사이의 원자성 문제

② Transaction Log Tailing (CDC)  ← 이 프로젝트
   Debezium이 MySQL binlog를 읽어 자동 발행

   필요한 컬럼:  없음 (INSERT만 하면 끝)
   장점: 실시간, DB에 추가 부하 없음, 애플리케이션이 Kafka를 모른다
   단점: Debezium + Kafka Connect 인프라 필요
```

`status` 컬럼이 **없는 것이 실수가 아니라 방식의 차이**라는 걸 주석으로 남겨 두면,
나중에 보는 사람이 "발행 여부를 어떻게 아나?" 하고 헤매지 않습니다.

`created_at`에 `@CreatedDate`를 쓰지 않고 직접 넣은 것도 일관됩니다
(`OutboxEvent`에 `@EntityListeners`가 없으므로 Auditing이 동작하지 않습니다).

---

## 3. ★ 소비자 쪽 — Post 이벤트로 Elasticsearch를 채운다

여기서 흐름이 완성됩니다.

```
① PostServiceImpl.createPost()
     posts 테이블 INSERT + outbox_events INSERT   (같은 트랜잭션)
② Debezium이 binlog 감지
③ EventRouter SMT가 변환 → Kafka "community.post.events"
④ PostEventConsumer 수신
⑤ PostEventHandler → Elasticsearch 인덱싱
```

**즉 MySQL이 원본이고 Elasticsearch는 검색용 복제본**입니다.
animal-service와 같은 CQRS 구조인데, 동기화 방식이 다릅니다.

| | animal-service | community-service |
|---|---|---|
| ES 동기화 | 배치 Step 2에서 **전체 재인덱싱** | Outbox 이벤트로 **건별 인덱싱** |
| 실시간성 | 하루 1회 | 즉시 (수 초) |
| 복구 방식 | 다음 배치가 전체를 다시 씀 | `SyncScheduler`가 누락분을 채움 |

게시글은 **쓰고 나서 바로 검색되어야** 하므로 건별 실시간 방식이 맞습니다.

### 3-1. Debezium 헤더에서 메타데이터를 꺼낸다

[kafka/PostEventConsumer.java](../community-service/src/main/java/com/pawbridge/communityservice/kafka/PostEventConsumer.java)

```java
@KafkaListener(topics = "community.post.events", groupId = "${spring.kafka.consumer.group-id}")
public void consumePostEvent(ConsumerRecord<String, String> record) {
    String eventId   = extractHeader(record, "id");          // ← Kafka 헤더
    String eventType = extractHeader(record, "eventType");   // ← Kafka 헤더
    ...
}

private String extractHeader(ConsumerRecord<String, String> record, String key) {
    Header header = record.headers().lastHeader(key);
    if (header == null) throw new IllegalArgumentException("Header not found: " + key);
    return new String(header.value(), StandardCharsets.UTF_8);
}
```

**메시지 본문이 아니라 헤더에서 읽습니다.** 그 이유는 커넥터 설정에 있습니다.

[infrastructure/kafka/connectors/community-outbox-connector.json](../infrastructure/kafka/connectors/community-outbox-connector.json)

```json
"transforms.outbox.table.field.event.id": "event_id",
"transforms.outbox.table.fields.additional.placement": "type:header:eventType"
```

- `table.field.event.id: event_id` → Debezium EventRouter가 이 값을 **`id`라는 헤더**에 넣습니다.
- `additional.placement: type:header:eventType` → `type` 컬럼을 **`eventType` 헤더**로 옮깁니다.

**왜 헤더를 쓰나** — 메시지 본문(`payload`)은 순수한 도메인 데이터로 두고, 라우팅/중복판별 같은
**전달 메타데이터는 헤더에 두는** 것이 메시징의 일반적인 관례입니다. 소비자가 본문을 파싱하지 않고도
"어떤 종류의 이벤트인지"를 알 수 있어, 필요하면 파싱 없이 걸러낼 수 있습니다.

`lastHeader()`를 쓴 이유는 Kafka 헤더가 **같은 키를 여러 번 가질 수 있는** 구조이기 때문입니다.
보통 마지막 값이 가장 최신입니다.

⚠️ 헤더가 없으면 예외를 던집니다. 좋습니다 — `eventId`가 없으면 멱등성을 보장할 수 없으므로
처리하지 않는 것이 맞습니다(animal-service와 같은 판단).

### 3-2. `{schema, payload}` 이중 구조 — 커넥터 설정에 따라 코드가 달라진다

```java
Map<String, Object> debeziumMessage = objectMapper.readValue(payloadJson, Map.class);
Map<String, Object> payload = (Map<String, Object>) debeziumMessage.get("payload");   // ★ 한 겹 벗기기
```

Kafka Connect의 JSON 컨버터는 기본적으로 **스키마를 함께 보냅니다.**

```json
{
  "schema": { "type": "struct", "fields": [ ... ] },     ← 타입 정보
  "payload": { "postId": 1, "title": "...", ... }        ← 실제 데이터
}
```

그래서 `payload`를 한 번 더 꺼내야 합니다.

**여기서 두 커넥터의 설정 차이를 확인할 필요가 있습니다.**

| | user-outbox-connector | community-outbox-connector |
|---|---|---|
| `value.converter` | `JsonConverter` 명시 | (기본값 사용) |
| `value.converter.schemas.enable` | **`false`** | (설정 없음 → 기본 `true`) |
| 소비자 코드 | `Map<String,Object> payload` 로 바로 받음 | `get("payload")` 로 한 겹 벗김 |

두 소비자 코드가 다르게 생긴 이유가 이것입니다. **커넥터 설정이 소비자 코드 모양을 결정합니다.**
알고 맞춘 것이라면 문제없지만, **커넥터 설정을 통일해 두면 소비자 코드도 통일**됩니다.

```json
// 권장: 모든 커넥터에 명시적으로 같은 설정
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "false"
```

스키마는 용량을 크게 차지합니다. 게시글 본문 1KB에 스키마 2KB가 붙는 식이라
**Kafka 저장 용량과 네트워크가 3배**가 될 수 있습니다. `false`가 실용적입니다.

### 3-3. 멱등성 순서 — 세 서비스가 서로 다르게 짰다

[kafka/PostEventHandler.java](../community-service/src/main/java/com/pawbridge/communityservice/kafka/PostEventHandler.java)

```java
/**
 * Race Condition 방지:
 * - Elasticsearch 먼저, processed_events 나중
 * - 실패 시 재시도 가능하도록 순서 보장
 */
@Transactional
public void indexPost(String eventId, Map<String, Object> payload) {
    if (processedEventRepository.existsByEventId(eventId)) return;    // ① 중복 체크
    elasticsearchOperations.save(document);                           // ② 실제 작업
    processedEventRepository.save(ProcessedEvent.of(eventId, "POST_CREATED"));  // ③ 기록
}
```

**세 서비스의 순서를 비교해 보세요.**

| 서비스 | 순서 | 주석의 주장 |
|---|---|---|
| user-service (보상) | 체크 → **기록** → 처리 | *"ProcessedEvent 먼저 저장 (Race condition 방지)"* |
| animal-service | 체크 → 처리 → **기록** | (순서 언급 없음) |
| community-service | 체크 → 처리 → **기록** | *"Elasticsearch 먼저, processed_events 나중"* |

**정반대의 주장이 같은 프로젝트 안에 있습니다.** 누가 맞을까요? — **작업의 성질에 따라 다릅니다.**

```
작업이 멱등(여러 번 해도 결과가 같음)이면  →  처리 먼저, 기록 나중이 안전
   예: ES에 같은 _id로 save → 덮어쓰기라 두 번 해도 같은 결과
   기록이 실패해 재처리되어도 무해하다

작업이 비멱등(할 때마다 결과가 바뀜)이면  →  기록을 먼저 하고 트랜잭션으로 묶어야 한다
   예: favoriteCount++ → 두 번 하면 값이 틀어진다
```

**community-service는 옳습니다.** `elasticsearchOperations.save()`는 `postId`를 `_id`로 쓰므로
**같은 문서를 몇 번 저장해도 결과가 동일**합니다(upsert).
그래서 "처리 먼저"가 안전하고, 오히려 그게 더 나은 선택입니다.

**animal-service도 옳습니다.** `favoriteCount++`는 비멱등이지만
[`@Transactional`이 체크·처리·기록을 하나로 묶어](04-animal-service.md#5-3-멱등성--processedevent-테이블)
"카운트만 오르고 기록이 안 되는" 상태를 만들지 않습니다.

**user-service만 문제가 있었습니다.** 순서를 바꿔 놓고 try-catch로 중복을 잡으려 했는데
[JPA 쓰기 지연 때문에 그 catch가 동작하지 않았습니다](03-user-service.md#8-8-saga-보상-트랜잭션--되돌릴-수-없는-것을-되돌리는-방법).

> 💡 **정리**: "무엇을 먼저 하느냐"보다 **"작업이 멱등한가, 그리고 트랜잭션 경계가 어디인가"** 가
> 판단 기준입니다. 이 두 질문에 답할 수 있으면 순서는 자연히 정해집니다.

### 3-4. ⚠️ `@Transactional`이 Elasticsearch를 감싸지는 못한다

```java
@Transactional                              // ← MySQL 트랜잭션
public void indexPost(String eventId, Map<String, Object> payload) {
    elasticsearchOperations.save(document);  // ← Elasticsearch (트랜잭션 밖)
    processedEventRepository.save(...);       // ← MySQL (트랜잭션 안)
}
```

**Elasticsearch는 JPA 트랜잭션에 참여하지 않습니다.** 그래서 이런 순서가 가능합니다.

```
ES 저장 성공 → processed_events INSERT 실패 → MySQL 롤백
→ ES에는 문서가 있고 처리 기록은 없다
→ 재시도 → ES 다시 저장(덮어쓰기, 무해) → 기록 성공  ✅ 결국 맞춰진다
```

앞서 말한 대로 **ES 저장이 멱등이기 때문에 결과적으로 안전합니다.**
다만 `@Transactional`이 "ES까지 원자적으로 묶는다"는 착각을 줄 수 있으니,
주석으로 "ES는 트랜잭션 밖이며 멱등이라 안전하다"고 적어두면 다음 사람이 헷갈리지 않습니다.

### 3-5. 이벤트 3종의 처리

```java
switch (eventType) {
    case "POST_CREATED" -> postEventHandler.indexPost(eventId, payload);
    case "POST_UPDATED" -> postEventHandler.updatePost(eventId, payload);
    case "POST_DELETED" -> postEventHandler.deletePost(eventId, payload);
    default -> log.warn("⚠️ Unknown event type: {}", eventType);
}
```

Java 14의 **화살표 switch(`->`)** 를 써서 `break`가 필요 없습니다.
(animal-service는 전통적인 `case:` + `break`를 씁니다. 같은 프로젝트인데 스타일이 다릅니다.)

⚠️ `default`에서 **경고 로그만 남기고 넘어갑니다.** animal-service는 예외를 던집니다.

```java
// animal-service — 모르는 타입은 실패시킨다
default:
    throw new IllegalArgumentException("Unknown eventType: " + eventType);
```

새 이벤트 타입을 추가했는데 소비자를 배포하지 않았다면, animal-service 방식은 즉시 드러나고
community-service 방식은 **로그에 묻힙니다.** 후자가 더 위험합니다.

**`indexPost`와 `updatePost`가 거의 같은 코드입니다.**

```java
// indexPost
PostDocument document = PostDocument.builder().postId(...).authorId(...).title(...)...build();
elasticsearchOperations.save(document);
processedEventRepository.save(ProcessedEvent.of(eventId, "POST_CREATED"));

// updatePost — eventType 문자열만 다르다
PostDocument document = PostDocument.builder().postId(...).authorId(...).title(...)...build();
elasticsearchOperations.save(document);
processedEventRepository.save(ProcessedEvent.of(eventId, "POST_UPDATED"));
```

ES의 `save()`는 upsert이므로 **생성과 수정이 같은 동작**입니다. 사실상 하나로 합칠 수 있습니다.

```java
@Transactional
public void upsertPost(String eventId, String eventType, Map<String, Object> payload) {
    if (processedEventRepository.existsByEventId(eventId)) return;
    elasticsearchOperations.save(toDocument(payload));
    processedEventRepository.save(ProcessedEvent.of(eventId, eventType));
}
```

`toDocument(payload)`로 매핑을 한 곳에 모으면, 필드를 추가할 때 **두 곳을 고치다 하나를 빼먹는**
실수도 사라집니다.

### 3-6. ⚠️ 소비자가 `Map`에서 값을 캐스팅한다

```java
.postId(((Number) payload.get("postId")).longValue())
.authorId(((Number) payload.get("authorId")).longValue())
.title((String) payload.get("title"))
.imageUrls((List<String>) payload.get("imageUrls"))     // ← 검사되지 않는 캐스팅
```

`(Number)`로 받아 `.longValue()`를 호출하는 것은 **올바른 방어**입니다.
JSON 숫자는 Jackson이 `Integer`로도 `Long`으로도 만들 수 있어서, `(Long)`으로 바로 캐스팅하면
`ClassCastException`이 날 수 있습니다.

⚠️ 다만 전체 구조가 취약합니다.

- `payload.get("postId")`가 `null`이면 → `NullPointerException`
- 발행 측(`Map.of("postId", ...)`)에서 키 이름을 바꾸면 → 컴파일은 통과, 런타임에 실패
- `(List<String>)` 캐스팅은 제네릭이 지워져 **실제로는 검사되지 않습니다.**

**발행 측과 수신 측이 문자열 키로만 연결**되어 있어 컴파일러가 도와줄 수 없습니다.
[03-user-service.md의 같은 지적](03-user-service.md#8-7-mapof로-이벤트-페이로드를-만드는-부분)과 동일한 문제입니다.

```java
// 개선: 이벤트 계약을 클래스로 정의하고 양쪽이 공유
public record PostEventPayload(Long postId, Long authorId, String title,
                               String content, String boardType, List<String> imageUrls) {}

// 발행
outboxService.saveEvent("Post", id, "POST_CREATED", new PostEventPayload(...));
// 수신
PostEventPayload p = objectMapper.convertValue(payload, PostEventPayload.class);
```

서비스가 분리되어 있어 클래스를 공유하기 어렵다면, 최소한 **각 서비스 안에서라도** DTO를 쓰면
`(String)` 캐스팅과 오타 위험이 사라집니다.

### 3-7. 에러 핸들러 — 3회 재시도 후 버린다

[config/KafkaConsumerConfig.java](../community-service/src/main/java/com/pawbridge/communityservice/config/KafkaConsumerConfig.java)

```java
/**
 * 에러 핸들링:
 * - 3회 재시도 (1초 간격)
 * - 실패 시 로그만 남김 (보상 트랜잭션 없음)
 * - SyncScheduler가 주기적으로 누락 복구      ← ★ 이 문장이 핵심
 */
DefaultErrorHandler errorHandler = new DefaultErrorHandler(
        (record, exception) -> log.error("❌ Kafka message processing failed after retries: {}", record, exception),
        new FixedBackOff(1000L, 3L)
);
```

animal-service처럼 `DefaultErrorHandler`를 붙였지만 **Recoverer가 로그만 남깁니다.**
그리고 그 이유를 주석에 명시했습니다: **"SyncScheduler가 주기적으로 누락 복구"**.

**이것은 정당한 설계입니다** — 조건이 하나 있습니다.

```
찜 카운트(animal-service)      게시글 인덱싱(community-service)
· 실패하면 숫자가 영구히 틀림     · 실패해도 MySQL에는 있다
· 되돌릴 방법이 보상뿐            · 나중에 다시 인덱싱하면 복구된다
→ 보상 트랜잭션 필요              → 재동기화로 충분
```

**MySQL이 진실의 원천이고 ES는 복제본**이므로, 복제가 실패하면 **다시 복제하면 됩니다.**
보상 트랜잭션이라는 무거운 장치가 필요 없습니다. 판단이 정확합니다.

### 3-8. ⚠️ 그런데 SyncScheduler가 "누락"만 복구한다

[scheduler/SyncScheduler.java](../community-service/src/main/java/com/pawbridge/communityservice/scheduler/SyncScheduler.java)

```java
@Scheduled(cron = "0 0 2 * * ?")     // 매일 새벽 2시
public void syncPostsToElasticsearch() {
    List<Post> posts = postRepository.findByDeletedAtIsNullOrderByCreatedAtDesc();
    for (Post post : posts) {
        // Elasticsearch에 문서가 없으면 재인덱싱
        if (!elasticsearchOperations.exists(String.valueOf(post.getPostId()), PostDocument.class)) {
            elasticsearchOperations.save(document);
        }
    }
}
```

**`exists()`로 "있는지"만 확인합니다.** 그래서 복구되는 것과 안 되는 것이 갈립니다.

| 유실된 이벤트 | ES 상태 | SyncScheduler가 복구? |
|---|---|---|
| `POST_CREATED` | 문서 없음 | ✅ 복구됨 |
| `POST_UPDATED` | **옛 내용의 문서 있음** | ❌ `exists`가 true → 건너뜀 |
| `POST_DELETED` | **삭제됐어야 할 문서 있음** | ❌ 목록에 없으니 아예 검사 대상 아님 |

**결과**

- 제목을 수정했는데 UPDATE 이벤트가 유실되면, **검색 결과에 영구히 옛 제목이 남습니다.**
- 게시글을 삭제했는데 DELETE 이벤트가 유실되면, **삭제된 글이 검색에 계속 노출됩니다.**
  다행히 검색 서비스가 MySQL로 한 번 더 확인하므로([4-2](#4-2-es로-찾고-mysql로-읽는다))
  최종 노출은 막힙니다. 하지만 ES에는 쓰레기 문서가 남습니다.

**고치는 방법 두 가지**

```java
// (A) exists 대신 항상 덮어쓴다 — 가장 간단 (ES save는 upsert라 안전)
for (Post post : posts) {
    elasticsearchOperations.save(toDocument(post));   // exists 검사 제거
}
// 그리고 삭제분 처리: MySQL에 없는 ES 문서를 지운다
Set<Long> liveIds = posts.stream().map(Post::getPostId).collect(toSet());
// ES 전체 id를 스캔해 liveIds에 없는 것 삭제

// (B) updated_at 을 문서에 넣고 비교한다 — 효율적
// PostDocument 에 updatedAt 필드 추가
// MySQL의 updatedAt 이 더 최신이면 재인덱싱
```

**(A)가 이 규모에서 낫습니다.** 게시글 수가 수만 건 이하라면 매일 전체 덮어쓰기가 가장 단순하고
확실합니다(animal-service의 `reindexAllAnimals`가 이미 그 방식입니다).

⚠️ **성능 문제도 함께 있습니다.**

```java
List<Post> posts = postRepository.findByDeletedAtIsNullOrderByCreatedAtDesc();  // 전체를 메모리에
for (Post post : posts) {
    elasticsearchOperations.exists(...)       // 게시글 1건당 ES 호출 1번
    elasticsearchOperations.save(document)    // 저장도 1건씩
}
```

- 게시글 전체를 **한 번에 메모리에** 올립니다. `content`가 TEXT라 10만 건이면 위험합니다.
- ES 호출이 **건당 1~2회**입니다. Bulk API를 쓰면 수백 건을 한 번에 보낼 수 있습니다.

```java
// 개선: 페이징 + Bulk
Pageable page = PageRequest.of(0, 500);
Page<Post> chunk;
do {
    chunk = postRepository.findByDeletedAtIsNull(page);
    List<IndexQuery> queries = chunk.map(this::toIndexQuery).toList();
    elasticsearchOperations.bulkIndex(queries, PostDocument.class);   // 한 번에
    page = page.next();
} while (chunk.hasNext());
```

### 3-9. CleanupScheduler — 시간대 배치

[scheduler/CleanupScheduler.java](../community-service/src/main/java/com/pawbridge/communityservice/scheduler/CleanupScheduler.java)

```java
@Scheduled(cron = "0 0 3 * * ?")   // 03:00 — outbox_events 7일
@Scheduled(cron = "0 0 4 * * ?")   // 04:00 — processed_events 30일
```

user-service와 같은 정책(7일/30일)이고 시간만 다릅니다.
`SyncScheduler`(02:00) → outbox 정리(03:00) → processed 정리(04:00) 순서로 겹치지 않게 배치했습니다.

주석의 한 문장이 중요합니다.

```java
/** 참고: Debezium은 INSERT만 처리하므로 DELETE 이벤트는 무시됨 */
```

**정리 작업이 새로운 Kafka 이벤트를 만들지 않는다**는 확인입니다.
Debezium은 기본적으로 INSERT/UPDATE/DELETE를 모두 잡지만,
**EventRouter SMT는 outbox 테이블의 DELETE를 무시**합니다(발행할 payload가 없으므로).
이걸 확인하지 않고 정리 배치를 돌리면 "정리할 때마다 이벤트가 재발행되는" 사고가 날 수 있습니다.

---

## 4. Elasticsearch 검색

### 4-1. `@Field(analyzer = "nori")` — animal-service와 다른 방식

[elasticsearch/PostDocument.java](../community-service/src/main/java/com/pawbridge/communityservice/elasticsearch/PostDocument.java)

```java
@Document(indexName = "posts")
public class PostDocument {
    @Id @Field(type = FieldType.Long)
    private Long postId;

    @Field(type = FieldType.Text, analyzer = "nori")     // ★ 한국어 형태소 분석
    private String title;
    @Field(type = FieldType.Text, analyzer = "nori")
    private String content;

    @Field(type = FieldType.Keyword)
    private String boardType;
    @Field(type = FieldType.Keyword)
    private List<String> imageUrls;
}
```

`Text` + `Keyword` 구분은 [04-animal-service.md](04-animal-service.md#4-4-animaldocument--es-문서-설계)와
같은 원칙입니다. 사람이 검색하는 값은 `Text`, 정확히 일치해야 하는 값은 `Keyword`.

⚠️ **다만 두 서비스가 nori를 서로 다른 방식으로 쓰고 있습니다.**

| | animal-service | community-service |
|---|---|---|
| 인덱스 생성 | 매핑 JSON을 손으로 적용 (`setup-index.sh`) | Spring Data가 **자동 생성** |
| 분석기 | 커스텀 `nori_analyzer` | 내장 `nori` |
| 사용자 사전 | ⭕ 품종 12개 등록 | ❌ 없음 |
| 품사 필터 | ⭕ 조사·어미 제거 | ❌ 없음 |

**결과: 게시글 검색은 품종 이름을 제대로 처리하지 못합니다.**

```
"믹스견 찾아요"  →  내장 nori는 ["믹스", "견", "찾다"] 로 쪼갠다
                    → "믹스견"으로 검색하면 잘 안 맞는다
```

동물 인덱스에는 이 문제를 해결한 사용자 사전이 이미 있으므로, **같은 설정을 posts 인덱스에도
적용하면 됩니다.**

```java
// 방법 1: 인덱스 설정 파일을 지정한다
@Document(indexName = "posts")
@Setting(settingPath = "elasticsearch/posts-settings.json")   // nori_analyzer 정의
public class PostDocument {
    @Field(type = FieldType.Text, analyzer = "nori_analyzer")
    private String title;
```

```bash
# 방법 2: animal 쪽처럼 인덱스를 미리 만들어 두고 자동 생성을 끈다
#   (Spring Data는 이미 있는 인덱스는 건드리지 않는다)
```

### 4-2. ES로 찾고 MySQL로 읽는다

[service/SearchServiceImpl.java](../community-service/src/main/java/com/pawbridge/communityservice/service/SearchServiceImpl.java)

```java
/**
 * 검색 로직:
 * 1. Elasticsearch에서 title 또는 content에 keyword 포함된 문서 검색
 * 2. postId 목록 추출
 * 3. MySQL에서 실제 데이터 조회 (최신 데이터 보장)
 */
NativeQuery query = NativeQuery.builder()
        .withQuery(q -> q.multiMatch(m -> m
                .query(keyword)
                .fields("title", "content")      // 두 필드 동시 검색
                .analyzer("nori")))
        .build();

SearchHits<PostDocument> searchHits = elasticsearchOperations.search(query, PostDocument.class);
List<Long> postIds = searchHits.getSearchHits().stream()
        .map(SearchHit::getContent).map(PostDocument::getPostId).toList();

return postRepository.findAllById(postIds).stream()          // ★ MySQL에서 다시 조회
        .filter(post -> post.getDeletedAt() == null)
        .map(post -> PostResponse.fromEntity(post, getUserNickname(post.getAuthorId())))
        .toList();
```

**이 패턴에는 정식 이름이 있습니다 — ES를 "검색 인덱스"로만 쓰고 데이터는 원본에서 읽는 방식입니다.**

```
얻는 것
· ES가 몇 초 뒤처져 있어도 사용자는 항상 최신 내용을 본다
· ES에 남아 있는 삭제된 글이 노출되지 않는다 (deletedAt 필터)
· ES 문서에 모든 필드를 넣지 않아도 된다 (인덱스가 작아짐)

잃는 것
· 조회가 2단계 (ES → MySQL)
· ★ 관련도 순서가 사라진다   ← 아래 참고
```

⚠️ **`findAllById()`는 순서를 보장하지 않습니다.**

ES는 검색 결과를 **관련도(점수) 높은 순**으로 돌려줍니다. 그게 검색엔진을 쓰는 이유입니다.
그런데 `findAllById(postIds)`는 내부적으로 `WHERE post_id IN (...)`이므로
**DB가 편한 순서(보통 PK 순)로 반환**합니다. **검색 순위가 통째로 버려집니다.**

```java
// 개선: ES가 준 순서를 유지한다
Map<Long, Post> map = postRepository.findAllById(postIds).stream()
        .filter(p -> p.getDeletedAt() == null)
        .collect(Collectors.toMap(Post::getPostId, Function.identity()));

return postIds.stream()             // ★ ES 순서로 순회
        .map(map::get)
        .filter(Objects::nonNull)
        .map(post -> PostResponse.fromEntity(post, ...))
        .toList();
```

⚠️ **페이징이 없습니다.** `NativeQuery`에 `Pageable`을 주지 않으면
**Spring Data Elasticsearch의 기본 크기(10건)** 만 반환합니다.

```java
// 사용자는 "검색 결과가 10개뿐"이라고 느낀다. 총 건수도 알 수 없다.
NativeQuery query = NativeQuery.builder()
        .withQuery(...)
        .withPageable(pageable)      // ★ 이게 필요하다
        .build();
```

전체 건수는 `searchHits.getTotalHits()`로 얻을 수 있으므로 `Page` 응답을 만들 수 있습니다.

### 4-3. `multi_match` — 여러 필드를 한 번에

```java
.multiMatch(m -> m.query(keyword).fields("title", "content"))
```

`match` 쿼리를 필드별로 만들어 `should`로 묶는 것과 같은 일을 한 줄로 합니다.

```json
// multi_match 가 대신해 주는 것
{ "bool": { "should": [
    { "match": { "title":   "강아지" }},
    { "match": { "content": "강아지" }}
]}}
```

`multi_match`에는 여러 타입이 있어서, 실무에서는 **제목에 가중치를 주는** 방식을 자주 씁니다.

```java
.fields("title^3", "content")     // 제목 일치를 3배 중요하게
```

제목에 키워드가 있는 글이 본문에만 있는 글보다 위로 올라옵니다. 지금은 동일 가중치입니다.

---

## 5. ⚠️ N+1 HTTP 호출 — MSA에서 가장 비싼 실수

### 5-1. 문제

게시글에는 작성자 **닉네임**을 보여줘야 합니다. 그런데 닉네임은 user-service에 있습니다.

[client/UserServiceClient.java](../community-service/src/main/java/com/pawbridge/communityservice/client/UserServiceClient.java)

```java
@FeignClient(name = "user-service")
public interface UserServiceClient {
    @GetMapping("/api/v1/users/internal/{userId}/nickname")
    String getUserNickname(@PathVariable("userId") Long userId);
}
```

그리고 목록 조회에서 이렇게 씁니다.

```java
// PostServiceImpl.getAllPosts()
return postRepository.findByDeletedAtIsNullOrderByCreatedAtDesc().stream()
        .map(post -> {
            String authorNickname = getUserNickname(post.getAuthorId());   // 🚨 게시글마다 HTTP 호출
            return PostResponse.fromEntity(post, authorNickname);
        })
        .collect(Collectors.toList());
```

**게시글이 100개면 user-service에 HTTP 요청을 100번 보냅니다.**

```
호출 1번에 20ms 가정
  게시글 100개  →  100 × 20ms = 2초
  게시글 500개  →  10초
  게시글 1,000개 → 20초 → 타임아웃
```

같은 코드가 **4곳**에 있습니다: `getAllPosts()`, `getAllPostsForAdmin()`, `searchPosts()`,
그리고 개별 조회.

**게다가 `getAllPosts()`에는 페이징이 없습니다.**

```java
List<PostResponse> getAllPosts();     // 전체 게시글을 다 가져온다
```

게시글이 쌓이면 **DB에서 전체를 읽고, HTTP를 전체 건수만큼 호출하고, JSON으로 전부 직렬화**합니다.
서비스가 멈추는 가장 전형적인 경로입니다.

### 5-2. 실패 처리는 잘 되어 있다

```java
private String getUserNickname(Long userId) {
    try {
        return userServiceClient.getUserNickname(userId);
    } catch (Exception e) {
        log.warn("Failed to fetch nickname for userId={}, using default. Error: {}", userId, e.getMessage());
        return "사용자" + userId;      // ★ 기본값으로 대체
    }
}
```

**Graceful Degradation입니다.** user-service가 죽어도 게시글 목록은 보입니다.
작성자만 "사용자12"로 표시됩니다. 목록 전체가 500이 되는 것보다 훨씬 낫습니다.

⚠️ **그런데 N+1과 결합하면 최악이 됩니다.**

```
user-service가 죽으면:
  게시글 100개 × (Feign 연결 타임아웃 5초) = 500초
  → 게시글 목록 요청이 8분 넘게 걸리다가 게이트웨이 타임아웃
  → "기본값으로 버티기"가 오히려 서비스를 마비시킨다
```

이것이 [03-user-service.md](03-user-service.md#실패-처리-방식이-두-갈래로-갈려-있다)에서 언급한
**Circuit Breaker가 실제로 필요한 상황**입니다. try-catch fallback은 "한 번의 실패"를 견디지만,
"계속되는 실패"에는 호출 자체를 차단해야 합니다.

### 5-3. 고치는 방법 (효과 순)

```java
// ① 일괄 조회 엔드포인트 — 호출 100번 → 1번  ★ 가장 효과가 크다
@PostMapping("/api/v1/users/internal/nicknames")
Map<Long, String> getNicknames(@RequestBody List<Long> userIds);

// 사용
List<Post> posts = postRepository.findByDeletedAtIsNull(pageable).getContent();
Set<Long> authorIds = posts.stream().map(Post::getAuthorId).collect(toSet());  // 중복 제거!
Map<Long, String> nicknames = fetchNicknames(authorIds);       // HTTP 1번
return posts.stream()
        .map(p -> PostResponse.fromEntity(p, nicknames.getOrDefault(p.getAuthorId(), "사용자" + p.getAuthorId())))
        .toList();
```

`Set`으로 중복을 제거하는 것도 중요합니다. 한 사람이 게시글 10개를 썼으면 지금은 **같은 닉네임을
10번** 물어봅니다.

user-service에는 이미 [일괄 조회의 선례가 있습니다](03-user-service.md#n1-네트워크-호출을-피한-설계)
(`POST /api/v1/mypage/animals/batch`). 같은 방식을 적용하면 됩니다.

```java
// ② 페이징 필수
Page<PostResponse> getAllPosts(Pageable pageable);

// ③ 닉네임 캐싱 — 닉네임은 거의 바뀌지 않는다
@Cacheable(value = "nicknames", key = "#userId")
public String getUserNickname(Long userId) { ... }
// Redis가 이미 인프라에 있으므로 Spring Cache + Redis 로 바로 가능

// ④ 이벤트로 미리 받아두기 (비정규화)
// user.nickname.changed 이벤트를 구독해 posts 테이블이나 별도 테이블에 닉네임을 복사
// → 조회 시 HTTP 호출이 아예 사라진다. animal-service가 shelter_name 을 ES에 복사한 것과 같은 발상.
```

**①+②만 해도 실질적으로 해결됩니다.** ④는 가장 빠르지만 동기화 비용이 생기므로,
트래픽이 실제로 문제가 될 때 고려하는 게 맞습니다.

---

## 6. 그 외 눈여겨볼 점

### 6-1. ⚠️ 이미지 수정 순서 — 삭제가 업로드보다 먼저다

```java
// PostServiceImpl.updatePost()
if (images != null && images.length > 0) {
    List<String> oldImageUrls = post.getImageUrls();
    if (oldImageUrls != null && !oldImageUrls.isEmpty()) {
        oldImageUrls.forEach(s3Service::deleteFile);      // ① 기존 파일 삭제
    }
    newImageUrls = s3Service.uploadImages(images);         // ② 새 파일 업로드
}
```

**②가 실패하면 어떻게 되나요?**

```java
// S3ServiceImpl.uploadImages()
} catch (IOException e) {
    throw new RuntimeException("파일 업로드 중 오류가 발생했습니다", e);   // 예외를 던진다
}
```

예외가 나면 트랜잭션은 롤백되어 **DB의 `image_urls`는 옛 URL을 그대로 유지**합니다.
그런데 **그 URL이 가리키는 S3 파일은 이미 지워졌습니다.**

→ **게시글의 이미지가 전부 깨집니다. 되돌릴 방법이 없습니다.**

```java
// 개선: 업로드 먼저, 삭제 나중 (그리고 커밋 후에 삭제)
newImageUrls = s3Service.uploadImages(images);     // ① 먼저 올린다 (실패해도 기존 파일 온전)
post.update(request.title(), request.content(), newImageUrls);
// ② 트랜잭션 커밋 후에 옛 파일 삭제
TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
    @Override public void afterCommit() { oldImageUrls.forEach(s3Service::deleteFile); }
});
```

**원칙: 외부 시스템에 대한 되돌릴 수 없는 작업(파일 삭제, 메일 발송, 결제)은
트랜잭션이 커밋된 뒤에 한다.** 실패 시 남는 쓰레기 파일은 정리 배치로 처리하면 됩니다.

같은 문제가 `updatePostByAdmin()`에도 그대로 있습니다.

### 6-2. 관리자 메서드는 작성자 검증을 뺀다

```java
// 일반 사용자
if (!post.getAuthorId().equals(authorId)) {
    throw new UnauthorizedPostAccessException();
}

// 관리자 (updatePostByAdmin) — 이 검증이 없다
```

`AdminPostController`가 별도로 있고, 인가는
[게이트웨이의 `/api/v1/admin/**` 규칙](02-api-gateway.md#3-7-관리자-경로만-별도-처리)이 담당합니다.
컨트롤러 주석에도 적혀 있습니다: *"인증 및 권한 체크: API Gateway의 JWT 필터에서 ROLE_ADMIN 체크"*.

**이 경로는 실제로 안전합니다.** 관리자 라우트가 모두 `/api/v1/admin/...`으로 리라이트된 뒤
필터를 타므로 접두사 규칙이 정확히 적용됩니다.

다만 `X-User-Role` 헤더가 이미 전달되므로, **서비스에서 한 번 더 확인하면 이중 방어**가 됩니다.
[user-service의 마이페이지](03-user-service.md#권한-검증이-여기에도-있다)가 그렇게 하고 있습니다.

```java
@PatchMapping("/{postId}")
public ResponseEntity<?> updateByAdmin(@RequestHeader("X-User-Role") String role, ...) {
    if (!"ROLE_ADMIN".equals(role)) throw new UnauthorizedPostAccessException();
    ...
}
```

### 6-3. 댓글은 Elasticsearch에 인덱싱되지 않는다

`Post`만 `outbox_events`에 이벤트를 남깁니다. `Comment`는 남기지 않습니다.

**의도된 것으로 보이고 합리적입니다.** "댓글 내용으로 검색"은 요구사항이 아닐 수 있고,
댓글은 게시글보다 훨씬 많아 인덱스 비용이 큽니다.

다만 알아둘 결과가 하나 있습니다: **게시글을 삭제해도 그 글의 댓글은 그대로 남습니다.**
`deletePost()`가 댓글을 건드리지 않기 때문입니다. 게시글 조회가 막히니 사용자에게 보이지는 않지만,
`comments` 테이블에는 계속 쌓입니다. 정리 정책을 정해두는 게 좋습니다.

### 6-4. 공통 규약이 전파되어 있다

`util/ResponseDTO.java`, `util/CustomResponseUtil.java`가 user-service와 **같은 파일**입니다.
`exception/common/`도 `ApplicationException` + `ErrorCode` + `GlobalExceptionRestAdvice` 구조가 같습니다.

**규약이 전파된 것은 좋은 신호입니다.** 프론트엔드가 모든 서비스에서 같은 응답 형태를 받습니다.

⚠️ 다만 **복사이지 공유가 아닙니다.** `ResponseDTO`에 메서드를 추가하면 서비스마다 따로 넣어야 하고,
시간이 지나면 조금씩 달라집니다. 이 레포에서 반복적으로 나타나는 문제이고
[00-overview.md](00-overview.md#공유-부품이-없다는-것의-대가)에서 종합해 다룹니다.

### 6-5. 이모지 로그

```java
log.info("📥 Received event: eventId={}, eventType={}", eventId, eventType);
log.info("✅ Indexed post: postId={}, eventId={}", document.getPostId(), eventId);
log.error("❌ Failed to consume post event", e);
log.info("🧹 Cleaned up {} old outbox events", deleted);
log.info("🔄 Starting MySQL → Elasticsearch sync");
```

터미널에서 로그를 눈으로 훑을 때 **성공/실패/단계가 즉시 구분**됩니다.
animal-service의 [FATAL-ERROR 박스](04-animal-service.md#5-8-fatal-error-처리--보상마저-실패했을-때)와
같은 발상입니다.

⚠️ 운영에서 로그를 수집·검색하는 경우(ELK, Datadog 등) 이모지가 검색이나 파싱을 방해할 수 있습니다.
검색 키는 `eventId=` 처럼 텍스트로 두고 이모지는 장식으로만 쓰는 지금 방식이면 문제없습니다.

---

## 7. 이 문서에서 배울 개념 정리

| 개념 | 한 줄 설명 | 코드 위치 |
|---|---|---|
| **enum 게시판** | 사용자가 추가하지 않는 고정 목록은 테이블보다 enum | `BoardType` |
| **서비스 경계는 ID로** | 다른 서비스의 데이터에 FK를 걸 수 없다 | `authorId` |
| **Soft Delete** | 복구·무결성·감사를 위해 표시만 한다 | `deletedAt` |
| **Soft Delete의 함정** | 조회마다 조건을 붙여야 하고, 외부 파일은 함께 못 살린다 | ⚠️ 1절 |
| **JSON 컬럼 + Converter** | 생명주기가 같고 개별 조회가 없는 목록에 적합 | `StringListConverter` |
| **Converter는 빈 주입 불가** | JPA가 생성하므로 `static` 사용 | `StringListConverter` |
| **부분 수정(null=유지)** | PATCH 지원. 단 "비우기"를 표현할 수 없다 | `Post.update()` |
| **Polling vs CDC Outbox** | `status` 컬럼이 없는 것은 방식의 차이 | `OutboxEvent` 주석 |
| **Kafka 헤더 vs 본문** | 전달 메타데이터는 헤더, 도메인 데이터는 본문 | `extractHeader` |
| **`schemas.enable`** | 이 설정이 소비자 코드 모양을 결정한다 | 3-2 |
| **멱등 작업의 순서** | 멱등이면 "처리 먼저", 비멱등이면 "트랜잭션으로 묶기" | 3-3 ★ |
| **ES는 트랜잭션 밖** | `@Transactional`이 ES를 감싸지 못한다 | 3-4 |
| **보상 vs 재동기화** | 복제본이 깨진 것은 다시 복제하면 된다 | 3-7 ★ |
| **`exists` 기반 동기화의 한계** | UPDATE/DELETE 유실은 복구되지 않는다 | ⚠️ 3-8 |
| **Bulk 인덱싱** | 건별 호출 대신 묶어 보낸다 | 3-8 |
| **`Text` vs `Keyword`** | 형태소 검색 vs 정확 일치 | `PostDocument` |
| **분석기 불일치** | 두 인덱스가 다른 nori 설정을 쓰고 있다 | ⚠️ 4-1 |
| **ES로 찾고 DB로 읽기** | 최신성 보장 + 인덱스 축소. 단 순서를 잃기 쉽다 | ⚠️ 4-2 ★ |
| **`multi_match` 가중치** | `title^3`으로 제목 일치를 우대 | 4-3 |
| **N+1 HTTP 호출** | DB N+1보다 10~100배 비싸다 | 🚨 5절 ★ |
| **fallback의 역설** | 기본값 대체 + N+1 = 타임아웃 폭탄 | ⚠️ 5-2 |
| **외부 작업은 커밋 후에** | 파일 삭제·메일·결제는 되돌릴 수 없다 | ⚠️ 6-1 ★ |

---

## 8. 개선 우선순위

| 순위 | 항목 | 심각도 | 근거 |
|---|---|---|---|
| 1 | 닉네임 조회를 일괄 조회로 + 목록에 페이징 | **높음** | 게시글 수에 비례해 HTTP 호출 (5절) |
| 2 | 이미지 수정 시 업로드 먼저, 삭제는 커밋 후 | **높음** | 업로드 실패 시 기존 이미지 영구 소실 (6-1) |
| 3 | `SyncScheduler`를 항상 덮어쓰기로 + 삭제분 처리 | 중간 | UPDATE/DELETE 유실이 복구되지 않음 (3-8) |
| 4 | 검색에 페이징 추가 + ES 순서 유지 | 중간 | 결과가 10건으로 잘리고 관련도 순서 상실 (4-2) |
| 5 | Soft Delete 시 S3 파일은 지연 삭제 | 중간 | 복구 가능성이 절반만 지켜짐 (1절) |
| 6 | posts 인덱스에도 사용자 사전·품사 필터 적용 | 중간 | 품종명 검색 품질 (4-1) |
| 7 | `SyncScheduler`에 페이징 + Bulk 적용 | 중간 | 전체를 메모리에 + 건당 ES 호출 (3-8) |
| 8 | 이벤트 페이로드를 `Map` → record로 | 낮음 | 키 오타를 컴파일러가 못 잡는다 (3-6) |
| 9 | `indexPost`/`updatePost` 통합 | 낮음 | 중복 코드, 필드 추가 시 누락 위험 (3-5) |
| 10 | 모르는 `eventType`에 예외 던지기 | 낮음 | 배포 누락이 로그에 묻힌다 (3-5) |
| 11 | 커넥터의 `schemas.enable` 통일 | 낮음 | 소비자 코드 모양이 서비스마다 다름 (3-2) |
| 12 | 관리자 메서드에 역할 이중 확인 | 낮음 | 게이트웨이 단일 방어 (6-2) |

---

**다음 문서** → [06-store-service.md](06-store-service.md) — 주문·재고·분산 락, 그리고 이벤트가 유실되는 지점
