# 04. animal-service — 공공데이터 배치, Elasticsearch, 그리고 "제대로 만든" Kafka 소비자

> 이 서비스에는 다른 곳에 없는 기술이 세 개 있습니다: **Spring Batch**(대량 데이터 처리),
> **공공데이터 API 연동**, **Elasticsearch 검색**. 그리고 [03-user-service.md](03-user-service.md)에서
> 지적한 Kafka 소비자 문제들이 **여기서는 올바르게 해결**되어 있습니다. 비교하며 읽으면 얻는 게 많습니다.

```
animal-service/src/main/java/com/pawbridge/animalservice/
├── batch/           ★ Spring Batch — APMS 공공데이터 동기화
│   ├── job/         ApmsAnimalBatchJob      (Job/Step 정의)
│   ├── reader/      ApmsItemReader          (API 호출 + 페이징)
│   ├── processor/   AnimalItemProcessor     (DTO → Entity, upsert 판단)
│   ├── writer/      AnimalItemWriter        (일괄 저장)
│   └── scheduler/   ApmsAnimalSyncScheduler (매일 새벽 2시)
├── client/          ApmsApiClient           (Feign — 공공데이터포털)
├── entity/          Animal, Shelter, OutboxEvent, ProcessedEvent, SyncHistory, BaseTimeEntity
├── enums/           Species, Gender, NeuterStatus, AnimalStatus, ApiSource ... (코드 → enum 변환)
├── document/        AnimalDocument          ★ Elasticsearch 문서
├── mapper/          AnimalMapper(Entity→DTO), AnimalDocumentMapper(ES문서→DTO), ShelterMapper
├── facade/          AnimalFacade, ShelterFacade   ★ CQRS 진입점
├── specification/   AnimalSpecification     (JPA 동적 쿼리 — 구버전 검색)
├── service/         Command/Elasticsearch/Index/S3/Outbox/NoticeNumberGenerator
├── consumer/        FavoriteEventConsumer   ★ 잘 만든 Kafka 소비자
├── handler/         FavoriteEventHandler
├── config/          Batch, ES, Feign, Jpa, KafkaConsumer, KafkaProducer, S3, Scheduler
├── admin/           관리자 통계 (일별 등록 건수)
└── mypage/          마이페이지 전용 엔드포인트 (user-service가 Feign으로 호출)
```

---

## 1. 이 서비스가 푸는 문제

유기동물 정보는 우리가 만드는 데이터가 아닙니다. **정부가 운영하는 APMS(동물보호관리시스템)** 에 있고,
공공데이터포털의 `abandonmentPublicService_v2` API로 받아옵니다.

```yaml
# pawbridge-config-repo/animal-service-dev.yml
apms:
  api:
    base-url: "https://apis.data.go.kr/1543061/abandonmentPublicService_v2"
    service-key: '{cipher}d06dfd...'
```

그래서 이 서비스의 일은 세 가지입니다.

1. **가져오기** — 매일 API를 호출해 수만 건을 우리 DB에 맞춰 저장 (Spring Batch)
2. **찾게 하기** — 사용자가 "믹스견", "순한", "수원시" 같은 말로 검색 (Elasticsearch + 한국어 형태소 분석)
3. **함께 쓰기** — 보호소 회원이 직접 등록하고, 찜 카운트를 다른 서비스에서 받아 반영 (Kafka)

---

## 2. Spring Batch — 대량 데이터를 안전하게 처리하는 틀

### 2-1. 왜 그냥 for문으로 안 하나

가장 단순한 방법은 이겁니다.

```java
// ❌ 이렇게 하면 안 되는 이유
@Scheduled(cron = "0 0 2 * * *")
public void sync() {
    List<ApmsAnimal> all = apiClient.getAll();     // 수만 건을 메모리에 다 올림 → OOM
    for (ApmsAnimal a : all) {
        animalRepository.save(convert(a));          // 5만 번 개별 INSERT → 매우 느림
    }
}
// 그리고 3만 번째에서 터지면? 어디까지 됐는지 모른다. 처음부터 다시.
```

Spring Batch는 이 세 문제를 구조로 해결합니다.

| 문제 | Batch의 해법 |
|---|---|
| 메모리 폭발 | **Chunk 단위 처리** — 500건씩 읽고 저장하고 버린다 |
| 느린 저장 | **Chunk 단위 트랜잭션** — 500건을 한 번에 커밋 |
| 실패 시 재시작 | **JobRepository** — 실행 이력을 DB에 남겨 이어서 재실행 |
| 일부 데이터 오류 | **skip / retry 정책** — 한 건 때문에 전체가 죽지 않게 |

### 2-2. Job / Step / Chunk 3층 구조

```
Job (apmsAnimalSyncJob)                     ← 전체 작업 1회 실행 단위
├── Step 1 (apmsAnimalSyncStep)             ← 단계. Chunk 방식
│     반복: Reader 500번 → Processor 500번 → Writer 1번 → 커밋
│     └ Reader:    APMS API 호출 (페이징)
│     └ Processor: ApmsAnimal(DTO) → Animal(Entity)
│     └ Writer:    saveAll(500건)
└── Step 2 (elasticsearchIndexStep)         ← 단계. Tasklet 방식 (한 번에 끝나는 작업)
      MySQL 전체 → Elasticsearch 재인덱싱
```

[batch/job/ApmsAnimalBatchJob.java](../animal-service/src/main/java/com/pawbridge/animalservice/batch/job/ApmsAnimalBatchJob.java)

```java
@Bean
public Job apmsAnimalSyncJob() {
    return new JobBuilder("apmsAnimalSyncJob", jobRepository)
            .start(apmsAnimalSyncStep())      // ① API → MySQL
            .next(elasticsearchIndexStep())    // ② MySQL → ES
            .build();
}
```

**Step을 두 개로 나눈 이유**가 중요합니다. 하나로 합쳐 "저장하면서 바로 ES에도 넣기"를 하면,
DB 트랜잭션이 롤백됐는데 ES에는 이미 들어간 상황이 생깁니다
([03-user-service.md의 Dual Write](03-user-service.md#8-2-순진한-방법과-그-함정-dual-write)와 같은 문제).
**"MySQL을 확정한 뒤, 그것을 원본으로 삼아 ES를 맞춘다"** 로 나누면 MySQL이 항상 진실이 됩니다.

**Chunk 방식과 Tasklet 방식의 차이**:
- **Chunk** — "많은 아이템을 반복 처리". Reader/Processor/Writer 3단 구조.
- **Tasklet** — "한 덩어리 작업". `RepeatStatus.FINISHED`를 반환하면 끝. 인덱싱, 파일 삭제,
  프로시저 호출 같은 것에 씁니다.

### 2-3. CHUNK_SIZE = 500, PAGE_SIZE = 500 — 왜 같게 맞췄나

```java
// ApmsAnimalBatchJob
private static final int CHUNK_SIZE = 500;  // Reader의 PAGE_SIZE와 동일하게 설정 (메모리 효율)

// ApmsItemReader
private static final int PAGE_SIZE = 500;   // APMS API 페이지 크기 (Chunk와 동일하게 설정)
```

주석까지 서로를 가리키고 있습니다. 이유:

```
PAGE_SIZE=500, CHUNK_SIZE=500  →  API 1번 호출 = 커밋 1번   ✅ 딱 맞음

PAGE_SIZE=500, CHUNK_SIZE=100  →  API 1번 호출 후 5번 커밋
                                   → 커밋 사이에 API 호출이 없어 리더가 유휴 상태
PAGE_SIZE=100, CHUNK_SIZE=500  →  커밋 1번에 API 5번 호출
                                   → 한 트랜잭션이 길어지고, 중간 실패 시 5페이지가 통째로 롤백
```

**청크 크기 = 트랜잭션 크기 = 롤백 단위**입니다. 크면 커밋 오버헤드가 줄지만 실패 시 잃는 게 많고,
작으면 안전하지만 느립니다. 여기에 "API 페이지 크기"라는 외부 제약이 있으니 그것에 맞춘 것이 합리적입니다.

### 2-4. Reader — 상태를 들고 있는 컴포넌트

[batch/reader/ApmsItemReader.java](../animal-service/src/main/java/com/pawbridge/animalservice/batch/reader/ApmsItemReader.java)

```java
@Component
public class ApmsItemReader implements ItemReader<ApmsAnimal>, StepExecutionListener {
    private int currentPage = 1;
    private List<ApmsAnimal> currentItems = new ArrayList<>();
    private int currentIndex = 0;
    private boolean isExhausted = false;

    @Override
    public ApmsAnimal read() {              // ★ 한 번에 한 건씩 반환
        if (isExhausted) return null;                       // null = "더 없음" 신호
        if (currentIndex >= currentItems.size()) {           // 현재 페이지 소진
            loadNextPage();                                  // → 다음 페이지 API 호출
            currentIndex = 0;
        }
        if (currentItems.isEmpty()) { isExhausted = true; return null; }
        return currentItems.get(currentIndex++);
    }
```

**`ItemReader.read()`의 계약은 "한 건 반환, 끝났으면 null"** 입니다. 페이징이라는 개념이 없습니다.
그래서 "내부적으로 페이지를 캐시해 두고 하나씩 꺼내주다가, 다 꺼내면 다음 페이지를 가져오는"
어댑터를 직접 만든 것입니다. 발상이 정확합니다.

**`StepExecutionListener`를 함께 구현한 이유**:

```java
@Override
public void beforeStep(StepExecution stepExecution) {
    currentPage = 1;  currentItems = new ArrayList<>();
    currentIndex = 0; isExhausted = false;
}
```

`@Component`는 **싱글톤**입니다. 첫 실행이 끝나면 `isExhausted = true`, `currentPage = 100` 같은 상태가
남아 있어서, **다음날 스케줄이 돌 때 아무것도 읽지 못하고 즉시 끝납니다.**
`beforeStep`에서 초기화해 이 문제를 막았습니다. 실무에서 자주 겪는 함정을 정확히 짚었습니다.

> 💡 **더 나은 방법**: `@StepScope`를 붙이면 Step 실행마다 새 인스턴스가 만들어져 초기화가 필요 없습니다.
> ```java
> @Component
> @StepScope        // Step마다 새 빈 → 상태 오염 원천 차단
> public class ApmsItemReader implements ItemReader<ApmsAnimal> { ... }
> ```
> 지금 방식은 동작하지만, **두 Job이 동시에 돌면 상태가 섞입니다**(같은 인스턴스를 공유하므로).

### 2-5. ⚠️ Reader의 예외 처리가 배치를 조용히 성공시킨다

```java
private void loadNextPage() {
    try {
        ... API 호출 ...
    } catch (Exception e) {
        log.error("APMS API 호출 중 오류 발생 - 페이지: {}", currentPage, e);
        currentItems = new ArrayList<>();      // ← 🚨 빈 리스트로 만들고 끝
    }
}
```

빈 리스트가 되면 `read()`가 `isExhausted = true`로 바꾸고 `null`을 반환합니다.
**Batch는 "데이터가 다 끝났다"고 판단하고 Step을 정상 종료(`COMPLETED`)합니다.**

즉 3페이지째에서 네트워크가 한 번 끊기면:

```
1페이지(500건) ✅  2페이지(500건) ✅  3페이지 💥 API 오류
→ 로그에 error 한 줄
→ Step 상태: COMPLETED (성공!)
→ Job 상태: COMPLETED (성공!)
→ 나머지 수만 건은 그날 들어오지 않는다. 아무도 모른다.
```

`ApmsAnimalSyncScheduler`도 `jobExecution.getStatus()`를 로그로만 찍으므로 알림이 없습니다.

**고치는 방법**

```java
// (A) 예외를 던져서 Step을 실패시킨다
} catch (Exception e) {
    log.error("APMS API 호출 실패 - 페이지: {}", currentPage, e);
    throw new ApmsSyncException("APMS 동기화 실패: page=" + currentPage, e);
}
```

그러면 Step이 `FAILED`가 되고, Batch가 실행 이력에 남기므로 **재시작하면 실패 지점부터 이어서**
처리할 수 있습니다(그게 JobRepository를 쓰는 이유입니다).

일시적 오류에는 재시도를 붙일 수도 있습니다.

```java
// (B) Step에 retry 정책 추가 — 일시적 네트워크 오류를 견딘다
.faultTolerant()
.retry(FeignException.class).retryLimit(3)
```

그리고 스케줄러에서 실패를 감지해 알립니다.

```java
if (jobExecution.getStatus() != BatchStatus.COMPLETED) {
    log.error("APMS 동기화 실패: {}", jobExecution.getAllFailureExceptions());
    // Slack/이메일 알림
}
```

### 2-6. Processor — 외부 데이터를 우리 모델로 번역하는 곳

[batch/processor/AnimalItemProcessor.java](../animal-service/src/main/java/com/pawbridge/animalservice/batch/processor/AnimalItemProcessor.java)

공공 API 데이터는 **정갈하지 않습니다.** 실제 응답을 주석으로 남겨 놓은 것이 좋은 기록입니다.

```java
/** 품종명 추출 — "[개] 믹스견" → "믹스견" */
private String extractBreedName(String kindNm) {
    if (kindNm.contains("]")) return kindNm.substring(kindNm.indexOf("]") + 1).trim();
    return kindNm.trim();
}

/** 출생연도 추출 — "2023(년생)" → 2023,  "2025(60일미만)(년생)" → 2025 */
private static final Pattern BIRTH_YEAR_PATTERN = Pattern.compile("(\\d{4})");
private Integer extractBirthYear(String age) {
    Matcher m = BIRTH_YEAR_PATTERN.matcher(age);
    return m.find() ? Integer.parseInt(m.group(1)) : null;
}

/** 날짜 파싱 — "20251109" → LocalDate */
private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
```

**핵심 원칙: 외부 데이터의 지저분함이 도메인 안쪽으로 스며들지 않게 경계에서 막는다.**
`Animal` 엔티티는 `birthYear`가 깔끔한 `Integer`이고, `"2025(60일미만)(년생)"` 같은 문자열은
Processor 밖으로 나가지 않습니다.

**코드 → enum 변환**도 같은 발상입니다.

```java
.species(Species.fromCode(apmsAnimal.getUpKindCd()))     // "417000" → DOG
.gender(Gender.fromCode(apmsAnimal.getSexCd()))          // "M" → MALE, "Q" → UNKNOWN
.neuterStatus(NeuterStatus.fromCode(apmsAnimal.getNeuterYn()))  // "Y"/"N"/"U"
```

`"417000"`이라는 숫자가 무슨 뜻인지 아는 곳은 `Species.fromCode()` 한 곳뿐입니다.
서비스 코드는 `Species.DOG`만 다룹니다.

### 2-7. Upsert — "있으면 수정, 없으면 생성"

```java
Animal animal = animalRepository.findByApmsDesertionNo(apmsAnimal.getDesertionNo()).orElse(null);
if (animal == null) {
    animal = createNewAnimal(apmsAnimal, shelter);     // 신규
} else {
    updateExistingAnimal(animal, apmsAnimal, shelter);  // 기존 갱신
}
return animal;
```

매일 같은 데이터를 다시 받으므로 **매번 INSERT하면 중복이 쌓입니다.** APMS의 `desertionNo`(유기번호)를
자연키로 삼아 존재 여부를 판단합니다. 그래서 엔티티에 이 제약이 있습니다:

```java
@Column(unique = true, length = 50)
private String apmsDesertionNo;
```

**`updateFromApms()`가 특히 중요합니다.**

```java
/**
 * APMS 배치 데이터로 업데이트
 * - APMS에서 변경될 수 있는 필드만 업데이트
 * - ID, desertionNo, noticeNo, apiSource, favoriteCount, description 등은 유지
 */
public void updateFromApms(String breed, Integer birthYear, ..., AnimalStatus status) { ... }
```

**우리가 만든 데이터를 배치가 덮어쓰지 않게 막는 장치**입니다.
`favoriteCount`(사용자들이 찜한 횟수)와 `description`(보호소가 직접 쓴 글)은 APMS에 없는 값입니다.
전체 필드를 통째로 갱신하면 매일 새벽 2시에 **찜 수가 0으로 초기화되고 보호소가 쓴 글이 사라집니다.**
"어떤 필드가 누구의 소유인가"를 명시적으로 구분한 것이 이 메서드의 핵심 가치입니다.

같은 발상이 상태 필드에도 적용되어 있습니다.

```java
/** APMS 진행상태 (processState) — APMS의 원본 상태 */
private String apmsProcessState;      // "공고중", "보호중"

/** 자체 관리 상태 — apmsProcessState와 분리 관리
 *  예: APMS는 "공고중"이지만 우리 시스템에서는 "입양완료" */
@Enumerated(EnumType.STRING)
private AnimalStatus status;
```

**외부 시스템의 상태와 우리 시스템의 상태를 분리**했습니다. 외부 데이터를 연동할 때 반드시 마주치는
문제이고, 대부분 처음엔 하나로 합쳐 놓고 나중에 후회합니다.

### 2-8. Shelter 자동 생성

```java
private Shelter findOrCreateShelter(ApmsAnimal apmsAnimal) {
    String careRegNo = apmsAnimal.getCareRegNo();
    if (!StringUtils.hasText(careRegNo)) {
        careRegNo = "UNKNOWN";      // ← 보호소 정보가 없는 데이터를 위한 도피처
    }
    return shelterRepository.findByCareRegNo(careRegNo)
            .orElseGet(() -> shelterRepository.save(Shelter.builder()
                    .careRegNo(careRegNo)
                    .name(hasText(apmsAnimal.getCareNm()) ? apmsAnimal.getCareNm() : "알 수 없는 보호소")
                    ...build()));
}
```

`Animal.shelter`가 `nullable = false`이므로 보호소 없이는 동물을 저장할 수 없습니다.
공공 데이터에 `careRegNo`가 빠진 건이 있으니 `"UNKNOWN"` 보호소를 만들어 붙입니다.
**"불량 데이터를 버리지 않고 격리해서 받아들이는" 실용적 선택입니다.**

이 `careRegNo`가 [03-user-service.md](03-user-service.md#1-회원가입--순서가-곧-규칙이다)의
보호소 회원가입 검증(`existsByCareRegNo`)과 연결됩니다. 배치가 만들어 둔 보호소 목록에
자기 등록번호가 있어야 보호소 회원으로 가입할 수 있습니다.

### 2-9. ⚠️ FaultTolerant 설정이 실패를 감춘다

```java
.faultTolerant()
.skipLimit(100)
.skip(FeignException.class)
.skip(IllegalArgumentException.class)
.skip(DataAccessException.class)
.skip(Exception.class)          // ← 🚨 이 한 줄이 위의 세 줄을 무의미하게 만든다
```

`Exception`은 모든 예외의 부모입니다. **어떤 예외든 100건까지 조용히 넘어갑니다.**
NPE, 설정 오류, DB 커넥션 고갈 — 전부 "스킵"으로 처리됩니다.

게다가 Processor가 예외를 **자기가 먼저 잡아** 버립니다.

```java
} catch (Exception e) {
    log.error("ApmsAnimal 처리 중 오류 발생: desertionNo={}", ..., e);
    return null;         // ← 예외를 먹고 null 반환
}
```

**Batch에서 Processor가 `null`을 반환하면 "필터링(filtered)"으로 집계되고, "스킵(skipped)"이 아닙니다.**
즉 `skipLimit(100)`이 발동하지 않습니다. 5만 건이 전부 실패해도 Job은 `COMPLETED`입니다.

```
의도한 동작                              실제 동작
100건 실패 → Job FAILED → 알림          5만 건 실패 → filtered 5만 → Job COMPLETED
```

**고치는 방법**

```java
// (1) Processor에서 예외를 삼키지 말고 던진다 (skip 정책이 세도록)
public Animal process(ApmsAnimal apmsAnimal) throws Exception {
    if (!StringUtils.hasText(apmsAnimal.getDesertionNo())) {
        return null;          // 데이터가 없는 건 "필터링"이 맞다
    }
    ... 변환 ...              // 예외는 그대로 전파 → skip 정책이 카운트
}

// (2) skip 대상을 좁힌다
.skip(IllegalArgumentException.class)   // 파싱 오류만
.skip(DataIntegrityViolationException.class)
.skipLimit(100)
// .skip(Exception.class) 제거 → 예상치 못한 오류는 Job을 실패시킨다

// (3) 스킵된 건을 기록한다
.listener(new SkipListener<ApmsAnimal, Animal>() {
    @Override public void onSkipInProcess(ApmsAnimal item, Throwable t) {
        skipLogRepository.save(...);   // 나중에 확인 가능
    }
})
```

> 💡 **원칙**: `skip`은 **"이런 종류의 실패는 예상했고 무시해도 된다"** 는 선언입니다.
> `Exception`을 스킵하는 건 "모든 실패를 예상했다"는 뜻이 되어 선언의 의미가 사라집니다.

### 2-10. 스케줄러 — `timestamp` 파라미터를 넣는 이유

[batch/scheduler/ApmsAnimalSyncScheduler.java](../animal-service/src/main/java/com/pawbridge/animalservice/batch/scheduler/ApmsAnimalSyncScheduler.java)

```java
@Scheduled(cron = "${batch.apms.sync.cron:0 0 2 * * ?}")
public void syncApmsAnimalData() {
    JobParameters jobParameters = new JobParametersBuilder()
            .addLong("timestamp", System.currentTimeMillis())   // ★
            .toJobParameters();
    JobExecution jobExecution = jobLauncher.run(apmsAnimalSyncJob, jobParameters);
}
```

**Spring Batch는 "같은 Job 이름 + 같은 파라미터"를 같은 JobInstance로 봅니다.**
그리고 **이미 성공한 JobInstance는 다시 실행할 수 없습니다**
(`JobInstanceAlreadyCompleteException`). 이건 "월급 계산 배치를 두 번 돌려 두 번 지급"하는
사고를 막기 위한 안전장치입니다.

그런데 이 배치는 **매일 같은 일을 해야 합니다.** 파라미터가 없으면 첫날만 실행되고 이후엔 전부 예외입니다.
그래서 `timestamp`를 넣어 **매번 다른 JobInstance**로 만듭니다.

> 실무에서는 `.addString("date", LocalDate.now().toString())`처럼 **날짜**를 넣는 편이 낫습니다.
> "오늘 배치가 이미 돌았는지"를 파라미터로 판별할 수 있고, 같은 날 두 번 실행되는 것도 막힙니다.
> `System.currentTimeMillis()`는 언제나 다른 값이라 그 보호가 사라집니다.

**설정 값에 기본값을 준 것도 좋은 습관입니다.**

```java
"${batch.apms.sync.cron:0 0 2 * * ?}"
//                    └ 콜론 뒤가 기본값 → 설정이 없어도 뜬다
```

Config Server에서 이 값을 바꾸면 재빌드 없이 스케줄을 조정할 수 있습니다.

### 2-11. `job.enabled: false` — 시작할 때 자동 실행을 막는다

```yaml
# animal-service-dev.yml
spring:
  batch:
    jdbc:
      initialize-schema: always
    job:
      enabled: false      # REST API로 수동 실행
```

Spring Boot는 기본적으로 **애플리케이션이 뜨자마자 등록된 모든 Job을 실행합니다.**
서비스를 재배포할 때마다 수만 건 배치가 도는 셈입니다. `enabled: false`로 끄고,
실행은 **스케줄러 또는 `/api/v1/batch/**` REST API**로만 합니다.

`initialize-schema: always`는 Batch가 실행 이력을 저장할 `BATCH_JOB_EXECUTION` 등의 테이블을
자동 생성하게 합니다. Batch가 재시작·이력 관리를 하려면 이 테이블이 필요합니다.

### 2-12. ⚠️ Processor의 조회가 건당 2번씩 발생한다

```java
public Animal process(ApmsAnimal apmsAnimal) {
    Shelter shelter = findOrCreateShelter(apmsAnimal);            // SELECT 1번
    Animal animal = animalRepository.findByApmsDesertionNo(...);   // SELECT 1번
```

500건 청크마다 **1,000번의 SELECT**가 발생합니다. 5만 건이면 10만 번입니다.
"청크로 묶어 성능을 얻는다"는 Batch의 이점을 Processor가 상당 부분 상쇄합니다.

**개선 방향**

```java
// Reader가 넘긴 500건의 desertionNo를 한 번에 조회해 Map으로 들고 처리
// (ItemProcessor 대신 ItemWriter에서 묶어 처리하거나, chunk listener에서 미리 로드)
Map<String, Animal> existing = animalRepository
        .findAllByApmsDesertionNoIn(desertionNos)      // SELECT 1번
        .stream().collect(toMap(Animal::getApmsDesertionNo, identity()));
```

보호소는 종류가 적으므로(수백 개) **Step 시작 시 전부 캐시**해 두면 조회가 사실상 사라집니다.

---

## 3. CQRS와 Facade — 읽기와 쓰기를 다른 저장소로

[facade/AnimalFacade.java](../animal-service/src/main/java/com/pawbridge/animalservice/facade/AnimalFacade.java)

클래스 주석에 전략이 명시되어 있습니다.

```
- Command (쓰기): MySQL (AnimalCommandService)
- Query  (읽기): Elasticsearch (AnimalElasticsearchService) — 목록/검색
- 상세 조회: MySQL (성능보다 데이터 정합성 우선)
```

### CQRS란

**Command Query Responsibility Segregation** — 쓰기 모델과 읽기 모델을 분리하는 것입니다.

```
        [쓰기]                            [읽기]
사용자 등록/수정 요청                  검색/목록 요청
      ↓                                    ↓
AnimalCommandService                AnimalElasticsearchService
      ↓                                    ↓
    MySQL  ──── Debezium CDC ────►  Elasticsearch
   (진실의 원천)                     (검색에 최적화된 복제본)
```

**왜 나누나** — 두 작업의 요구가 정반대입니다.

| | 쓰기 | 읽기(검색) |
|---|---|---|
| 필요한 것 | 트랜잭션, 제약조건, 정합성 | 형태소 분석, 정렬, 집계, 속도 |
| 잘하는 저장소 | RDB | 검색엔진 |
| 빈도 | 낮음 (배치/보호소 등록) | 매우 높음 (모든 사용자) |

MySQL에서 `WHERE special_mark LIKE '%순한%'`은 **인덱스를 못 씁니다**(앞에 `%`가 있으면 전체 스캔).
데이터가 늘면 검색이 급격히 느려집니다. Elasticsearch는 이 일을 위해 만들어진 도구입니다.

**"상세 조회만 MySQL"인 이유** — ES는 CDC로 동기화되므로 **수 초의 지연(eventual consistency)** 이
있습니다. 목록에서 몇 초 늦은 데이터가 보이는 건 괜찮지만, 사용자가 클릭해 들어간 상세 화면이
낡은 값이면 혼란스럽습니다. **화면의 성격에 따라 저장소를 골랐습니다.** 판단이 정확합니다.

### Facade 계층의 역할

```java
@Service
public class AnimalFacade {
    private final AnimalCommandService commandService;             // 쓰기
    private final AnimalElasticsearchService elasticsearchService;  // 읽기
    private final AnimalRepository animalRepository;               // 상세(MySQL)
    private final AnimalMapper animalMapper;
```

컨트롤러는 `AnimalFacade` 하나만 의존합니다. "이 조회는 ES에서, 저 조회는 MySQL에서"라는
**저장소 선택 지식이 컨트롤러로 새어나가지 않습니다.** 나중에 상세 조회도 ES로 옮기면
Facade 안에서만 바꾸면 됩니다.

⚠️ **작은 문제**: ES 조회 메서드에도 `@Transactional(readOnly = true)`가 붙어 있습니다.

```java
@Transactional(readOnly = true)
public Page<AnimalResponse> findExpiringSoonAnimals(Pageable pageable) {
    return elasticsearchService.findExpiringSoonAnimals(pageable);   // MySQL을 쓰지 않는다
}
```

Elasticsearch는 JPA 트랜잭션에 참여하지 않습니다. **DB 커넥션만 잡고 아무것도 안 하는** 셈이라,
트래픽이 많으면 커넥션 풀을 낭비합니다. ES 전용 메서드에서는 빼는 게 맞습니다.

---

## 4. Elasticsearch — 한국어 검색을 가능하게 하는 것들

### 4-1. 형태소 분석이 없으면 한국어 검색이 안 된다

영어는 공백으로 단어가 나뉩니다. 한국어는 아닙니다.

```
"순한 강아지를 찾아요"

공백 분리(standard):  ["순한", "강아지를", "찾아요"]
  → 사용자가 "강아지"로 검색하면 "강아지를"과 일치하지 않아 결과 0건 💀

형태소 분석(nori):    ["순하", "강아지", "찾다"]
  → "강아지" 검색 시 매칭 ✅
```

그래서 ES 이미지를 직접 빌드했습니다.

[infrastructure/elasticsearch/Dockerfile](../infrastructure/elasticsearch/Dockerfile)

```dockerfile
FROM docker.elastic.co/elasticsearch/elasticsearch:7.17.0
RUN elasticsearch-plugin install analysis-nori
```

`nori`는 Elastic이 공식 제공하는 한국어 형태소 분석 플러그인입니다. 기본 이미지에는 없으므로
플러그인을 설치한 커스텀 이미지를 만들어 Docker Hub에 올려 씁니다(`elasticsearch-nori`).

### 4-2. 사용자 사전 — 도메인 단어를 지켜내기

[infrastructure/elasticsearch/mappings/animals-index-mapping.json](../infrastructure/elasticsearch/mappings/animals-index-mapping.json)

```json
"nori_user_dict_tokenizer": {
  "type": "nori_tokenizer",
  "decompound_mode": "mixed",
  "user_dictionary_rules": [
    "믹스견", "시츄", "말티즈", "푸들", "치와와",
    "포메라니안", "요크셔테리어", "비글", "웰시코기",
    "리트리버", "진돗개", "삽살개"
  ]
}
```

nori는 일반 사전을 쓰므로 **개 품종 이름을 모릅니다.** 그대로 두면 이렇게 쪼개집니다.

```
"믹스견"  →  ["믹스", "견"]      → "믹스견"으로 검색해도 잘 안 맞는다
"웰시코기" → ["웰시", "코기"]
```

사용자 사전에 넣으면 **하나의 단어로 취급**됩니다. 이 프로젝트의 도메인(유기동물)을 이해하고 넣은
목록이라는 점이 좋습니다.

**`decompound_mode: mixed`** 는 복합어를 다루는 방식입니다.

```
"삼성전자" 를
  none    → ["삼성전자"]                 (안 쪼갬)
  discard → ["삼성", "전자"]              (쪼갠 것만)
  mixed   → ["삼성전자", "삼성", "전자"]   (원본 + 쪼갠 것 모두) ★
```

`mixed`는 색인 크기가 늘지만 **"골든리트리버"로도 "리트리버"로도 검색되게** 합니다.
검색 서비스에서 보통 옳은 선택입니다.

### 4-3. 품사 필터 — 의미 없는 조각 버리기

```json
"nori_posfilter": {
  "type": "nori_part_of_speech",
  "stoptags": ["E", "IC", "J", "MAG", "MAJ", "MM", "SP", "SSC", "SSO", "SC", "SE",
               "XPN", "XSA", "XSN", "XSV", "UNA", "NA", "VSV"]
}
```

주요 태그의 뜻:

| 태그 | 품사 | 예 |
|---|---|---|
| `J` | 조사 | 은/는/이/가/을/를 |
| `E` | 어미 | -다, -고, -습니다 |
| `IC` | 감탄사 | 아, 오 |
| `MAG` | 일반 부사 | 매우, 아주 |
| `MM` | 관형사 | 그, 이, 저 |
| `SP`/`SC`/`SE` | 구분 기호 | , · … |

**조사와 어미를 색인에서 빼면** 인덱스가 작아지고, "은/는" 같은 무의미한 토큰으로 검색될 일이
없어집니다. 검색 품질이 실제로 올라갑니다.

### 4-4. AnimalDocument — ES 문서 설계

[document/AnimalDocument.java](../animal-service/src/main/java/com/pawbridge/animalservice/document/AnimalDocument.java)

```java
@Document(indexName = "animals")
public class AnimalDocument {
    @Id
    private String esId;                                       // ES의 _id
    @Field(name = "id", type = FieldType.Long)
    private Long id;                                            // MySQL PK

    @Field(name = "apms_desertion_no", type = FieldType.Keyword)  // 정확 일치
    private String apmsDesertionNo;
    @Field(name = "breed", type = FieldType.Text)                 // 형태소 분석
    private String breed;
    @Field(name = "special_mark", type = FieldType.Text)
    private String specialMark;
    ...
    // 보호소 정보를 문서 안에 복사해서 넣는다 (비정규화)
    @Field(name = "shelter_name", type = FieldType.Text)
    private String shelterName;
    @Field(name = "shelter_address", type = FieldType.Text)
    private String shelterAddress;
}
```

**세 가지 설계 결정을 눈여겨볼 만합니다.**

#### ① Keyword vs Text — ES에서 가장 중요한 구분

```
Text     → 형태소 분석해서 쪼개 색인. 검색용. 정렬/집계 불가.
Keyword  → 통째로 색인. 정확 일치, 정렬, 집계, 필터링용.
```

이 문서의 선택을 보면 규칙이 뚜렷합니다.

| 필드 | 타입 | 이유 |
|---|---|---|
| `breed`, `special_mark`, `happen_place`, `color` | **Text** | 사람이 말로 검색하는 값 |
| `species`, `gender`, `status`, `neuter_status` | **Keyword** | enum 값 — 정확히 일치해야 하고 필터/집계 대상 |
| `apms_desertion_no`, `image_url` | **Keyword** | 식별자·URL — 쪼갤 이유 없음 |

`status`를 Text로 하면 `"ADOPTION_PENDING"`이 `["adoption", "pending"]`으로 쪼개져
**`status = "ADOPTED"` 필터가 오작동**합니다. 자주 겪는 실수인데 여기서는 정확히 구분했습니다.

#### ② 비정규화 — 보호소 정보를 복사해 넣는다

MySQL에서는 `animals`와 `shelters`가 FK로 나뉘어 있고 JOIN으로 붙입니다.
**Elasticsearch에는 JOIN이 없습니다**(정확히는 매우 제한적이고 느립니다).

그래서 문서 안에 보호소 이름·주소·전화번호를 **복사해 넣습니다.**

```
얻는 것: "수원시" 로 검색하면 보호소 주소까지 한 번에 검색된다. JOIN이 필요 없다.
잃는 것: 보호소 이름이 바뀌면 그 보호소의 동물 문서를 전부 갱신해야 한다.
```

검색 시스템에서는 **읽기 속도를 위해 중복을 감수하는 것이 일반적**입니다.
보호소 이름은 거의 바뀌지 않으므로 이 트레이드오프는 합리적입니다.

#### ③ 날짜를 Keyword 문자열로 저장한다

```java
@Field(name = "notice_end_date", type = FieldType.Keyword)
private String noticeEndDate;         // "2025-11-09"
```

주석에 *"날짜 필드는 String(Keyword)으로 매핑 (답변2 B안)"* 이라고 적혀 있습니다.
CDC로 들어오는 날짜 형식이 제각각(Debezium은 날짜를 epoch 정수나 문자열로 보냄)이라
**형식 불일치로 색인이 실패하는 것을 피하려는 선택**으로 보입니다.

**대가**: `FieldType.Date`가 아니므로 날짜 연산을 ES에서 못 합니다.

```
Date 타입이면:      "notice_end_date": { "lte": "now+3d" }     ← ES가 계산
Keyword면:          "notice_end_date": { "lte": "2025-11-12" } ← 애플리케이션이 계산해 넘겨야 함
```

`"2025-11-09"` 같은 ISO 형식은 **문자열 정렬 순서 = 날짜 순서**라서 범위 비교가 우연히 동작합니다.
동작하지만 취약한 기반입니다. CDC 파이프라인의 날짜 변환을 정리한 뒤 `Date` 타입으로 옮기는 게 좋습니다.

### 4-5. NativeQuery로 ES 쿼리 조립하기

[service/AnimalElasticsearchService.java](../animal-service/src/main/java/com/pawbridge/animalservice/service/AnimalElasticsearchService.java)

```java
// 단일 조건 (term = 정확 일치)
Query termQuery = Query.of(q -> q.term(t -> t.field("shelter_id").value(shelterId)));

// 복합 조건 (bool = AND/OR 조합)
BoolQuery boolQuery = BoolQuery.of(b -> b
    .must(Query.of(q -> q.term(t -> t.field("shelter_id").value(shelterId))))
    .must(Query.of(q -> q.term(t -> t.field("species").value(species.name()))))
);
```

`bool` 쿼리의 절(clause) 종류를 알아두면 유용합니다.

| 절 | 의미 | 점수 영향 |
|---|---|---|
| `must` | 반드시 만족 (AND) | ⭕ 점수에 반영 |
| `filter` | 반드시 만족 (AND) | ❌ 점수 무관, **캐시됨 → 빠름** |
| `should` | 만족하면 좋음 (OR) | ⭕ |
| `must_not` | 만족하면 안 됨 (NOT) | ❌ |

> 💡 **개선 지점**: `shelter_id`나 `species` 같은 **필터 조건은 `filter`를 쓰는 게 맞습니다.**
> `must`는 관련도 점수를 계산하는데, "보호소 ID가 5인가?"는 점수를 매길 대상이 아닙니다.
> `filter`는 점수 계산을 생략하고 결과를 캐시하므로 **더 빠릅니다.**
> 키워드 검색(`match`)만 `must`에 두고 나머지를 `filter`로 옮기면 성능이 개선됩니다.

### 4-6. ⚠️ 두 개의 검색 구현이 공존한다

`specification/AnimalSpecification.java`는 **MySQL 기반 동적 검색**입니다.
클래스 주석에 스스로 적어 두었습니다.

```java
/**
 * Animal 동적 검색 Specification
 * - Phase 1: MySQL 기반 임시 구현
 * - Phase 4: OpenSearch 전환 시 제거 예정
 */
```

내용은 JPA Criteria API로 조건을 조립하는 것입니다.

```java
predicates.add(criteriaBuilder.like(root.get("breed"), "%" + request.getBreed() + "%"));
```

`LIKE '%...%'`는 **인덱스를 못 타는 전체 스캔**입니다. 그래서 ES로 옮긴 것이고,
이 파일은 "제거 예정"인 유산입니다.

**다만 `fetchShelter()`에 배울 점이 하나 있습니다.**

```java
public static Specification<Animal> fetchShelter() {
    return (root, query, cb) -> {
        // 중복 fetch 방지: count 쿼리에서는 fetch join 제외
        if (query.getResultType() != Long.class && query.getResultType() != long.class) {
            root.fetch("shelter", JoinType.LEFT);
        }
        return cb.conjunction();
    };
}
```

Spring Data가 `Page`를 만들 때 **본문 조회 쿼리와 전체 개수(count) 쿼리를 각각 실행**합니다.
count 쿼리에 `fetch join`이 들어가면 `COUNT(...)`와 함께 JOIN이 붙어 오류가 나거나 비효율이 됩니다.
`query.getResultType()`으로 count 쿼리를 판별해 걸러낸 것은 **경험에서 나온 코드**입니다.

`cb.conjunction()`은 "항상 true"인 조건입니다. 이 Specification의 목적은 조건 추가가 아니라
**fetch join을 붙이는 것**뿐이므로, 조건 자리엔 무해한 값을 넣었습니다.

---

## 5. ★ Kafka 소비자 — 이 프로젝트에서 가장 잘 만든 부분

[03-user-service.md](03-user-service.md#8-3-해법-transactional-outbox-패턴)에서 시작한 찜 이벤트가
여기로 도착합니다. 그리고 **user-service·store-service의 소비자와 품질 차이가 큽니다.**

### 5-1. Delegator 패턴 — Kafka를 아는 층과 모르는 층을 나눈다

[consumer/FavoriteEventConsumer.java](../animal-service/src/main/java/com/pawbridge/animalservice/consumer/FavoriteEventConsumer.java)의
클래스 주석이 설계 의도를 그대로 담고 있습니다.

```
역할:
- 단일 토픽(user.favorite.events)에서 이벤트 수신
- eventType 필드로 이벤트 구분
- 파싱 및 검증만 수행, 비즈니스 로직은 Handler에 위임
- 예외 발생 시 throw → DefaultErrorHandler가 재시도 처리
- 재시도 실패 시 Recoverer가 보상 트랜잭션 발행
```

```
┌─────────────────────────────────────────────────────┐
│ FavoriteEventConsumer   ← Kafka를 안다                │
│  · @KafkaListener                                    │
│  · payload 파싱, 필수 필드 검증                        │
│  · ack.acknowledge()                                 │
│  · 비즈니스 로직은 없다                                │
└───────────────────────┬─────────────────────────────┘
                        │ 위임
┌───────────────────────▼─────────────────────────────┐
│ FavoriteEventHandler    ← Kafka를 모른다              │
│  · @Transactional                                    │
│  · 멱등성 체크 + 비즈니스 로직                          │
│  · 메서드명이 addFavorite / removeFavorite            │
└─────────────────────────────────────────────────────┘
```

**Handler 클래스 주석에 네이밍 규칙까지 적어 두었습니다.**

```
메서드 이름:
- handleXxx (X) - Kafka 용어
- addFavorite, removeFavorite (O) - 비즈니스 의미
```

**왜 이게 좋은 설계인가**

1. **Handler를 단위 테스트할 수 있습니다.** Kafka 없이 `addFavorite("id", 1L, 3L)`을 호출하면 됩니다.
   `@KafkaListener` 안에 로직이 있으면 테스트에 Kafka(또는 Embedded Kafka)가 필요합니다.
2. **트랜잭션 경계가 명확합니다.** `@Transactional`이 Handler에 있어서
   "멱등성 체크 + 카운트 증가 + 처리 기록"이 하나의 원자 단위입니다.
3. **나중에 전달 방식이 바뀌어도 로직은 그대로입니다.** Kafka를 RabbitMQ로 바꾸든 REST로 바꾸든
   Consumer만 교체합니다.

> ⚠️ 참고: `@KafkaListener` 메서드에 직접 `@Transactional`을 붙이면 **프록시를 거치지 않아
> 동작하지 않는 경우**가 있습니다(같은 클래스 내부 호출 문제와 유사). 별도 빈으로 분리한 이 구조는
> 그 함정도 자연히 피합니다.

### 5-2. 필수 필드 검증 — "없으면 아예 시작하지 않는다"

```java
String eventType = (String) payload.get("eventType");
String eventId   = (String) payload.get("eventId");

if (eventType == null || eventType.isBlank()) {
    log.error("[CONSUMER] Missing eventType, cannot route event: payload={}", payload);
    throw new IllegalArgumentException("eventType is required");
}
if (eventId == null || eventId.isBlank()) {
    log.error("[CONSUMER] Missing eventId, cannot guarantee idempotency: payload={}", payload);
    throw new IllegalArgumentException("eventId is required for idempotency");
}
```

**`eventId`가 없으면 처리를 거부하는 것이 핵심입니다.**
`eventId`는 중복 방지의 유일한 근거입니다. 없는 상태로 처리하면 중복 수신 시 카운트가 두 번 올라갑니다.
**"멱등성을 보장할 수 없으면 처리하지 않는다"** 는 판단이 명확합니다.

`default:` 절에서 모르는 `eventType`에도 예외를 던집니다.

```java
default:
    log.error("[CONSUMER] Unknown eventType: eventType={}, payload={}", eventType, payload);
    throw new IllegalArgumentException("Unknown eventType: " + eventType);
```

조용히 무시하지 않습니다. 새 이벤트 타입을 추가했는데 소비자를 배포하지 않은 경우
**로그와 재시도로 즉시 드러납니다.**

### 5-3. 멱등성 — ProcessedEvent 테이블

[handler/FavoriteEventHandler.java](../animal-service/src/main/java/com/pawbridge/animalservice/handler/FavoriteEventHandler.java)

```java
@Transactional
public void addFavorite(String eventId, Long userId, Long animalId) {
    // 1. 멱등성 체크 (@Transactional 내부에서 체크 → Race Condition 방지)
    if (processedEventRepository.existsByEventId(eventId)) {
        log.warn("[HANDLER] Duplicate event, skipping: eventId={}", eventId);
        return;                                  // 중복은 정상 처리로 간주
    }
    // 2. 비즈니스 로직
    animalCommandService.incrementFavoriteCount(animalId);
    // 3. 처리 완료 기록
    processedEventRepository.save(ProcessedEvent.of(eventId, "FAVORITE_ADDED"));
}
```

**Kafka는 "최소 한 번(at-least-once)" 전달**입니다. 같은 메시지가 두 번 올 수 있습니다.

```
왜 중복이 생기나:
  소비자가 메시지 처리 완료 → 오프셋 커밋하려는 순간 재시작
  → 커밋이 안 됐으니 재시작 후 그 메시지를 다시 받는다
```

`ProcessedEvent`에 `eventId`를 기록해 두고 이미 있으면 건너뜁니다.
**"중복은 정상 처리로 간주"하고 `return`하는 것도 옳습니다.** 예외를 던지면 재시도가 무한 반복됩니다.

**`@Transactional`로 1·2·3을 묶은 것이 결정적입니다.**

```
트랜잭션이 없다면:
  카운트 +1 성공 → ProcessedEvent 저장 실패 → 재시도 → 카운트 또 +1  💀

트랜잭션이 있으면:
  ProcessedEvent 저장이 실패하면 카운트 증가도 함께 롤백 → 재시도해도 안전 ✅
```

**user-service의 보상 핸들러와 비교해 보세요.** 거기는 "먼저 기록 → 나중에 처리" 순서에
try-catch를 얹었는데, [JPA 쓰기 지연 때문에 catch가 무력](03-user-service.md#8-8-saga-보상-트랜잭션--되돌릴-수-없는-것을-되돌리는-방법)했습니다.
여기는 **순서를 바꾸고 트랜잭션에 맡겨** 그 문제를 피했습니다. 같은 팀 코드인데 완성도가 다릅니다.

### 5-4. 수동 커밋 — 성공했을 때만 "읽음" 표시

```java
@KafkaListener(topics = "user.favorite.events", groupId = "animal-service-favorite-group")
public void consumeFavoriteEvent(Map<String, Object> payload, Acknowledgment ack) {
    ... 검증 ...
    switch (eventType) { ... favoriteEventHandler.addFavorite(...); ... }

    // 4. 처리 성공 시 수동 커밋
    //    Handler에서 예외 발생 시 이 라인에 도달하지 않음 → 오프셋 커밋 안 됨 → 재시도
    ack.acknowledge();
}
```

주석이 정확히 설명하고 있습니다. Handler가 예외를 던지면 `ack.acknowledge()` 줄에 **도달하지 않습니다.**
오프셋이 커밋되지 않으므로 그 메시지는 유실되지 않고 재시도됩니다.

### 5-5. ★ DefaultErrorHandler — 재시도 3회, 그다음 보상

[config/KafkaConsumerConfig.java](../animal-service/src/main/java/com/pawbridge/animalservice/config/KafkaConsumerConfig.java)

이 파일이 이 서비스에서 가장 정교한 코드입니다.

```java
private static final long RETRY_INTERVAL_MS = 1000L;   // 1초 간격
private static final long MAX_RETRY_ATTEMPTS = 3L;     // 최대 3회

@Bean
public CommonErrorHandler errorHandler() {
    DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            (record, ex) -> { /* ===== Recoverer: 모든 재시도 실패 시 ===== */ },
            new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRY_ATTEMPTS)
    );
    errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
            log.warn("[KAFKA-RETRY] Retry attempt {}/{} ...", deliveryAttempt, MAX_RETRY_ATTEMPTS, ...));
    return errorHandler;
}
```

**동작 흐름**

```
① 리스너가 예외를 던짐
② DefaultErrorHandler가 1초 뒤 재시도 (FixedBackOff)
③ 3회까지 재시도 — 그때마다 RetryListener가 로그
④ 그래도 실패 → Recoverer 람다 실행
```

**`FixedBackOff` vs `ExponentialBackOff`**

```
FixedBackOff(1000, 3)          →  1초, 1초, 1초
ExponentialBackOff(1000, 2.0)  →  1초, 2초, 4초, 8초...
```

DB 락 경합처럼 금방 풀리는 문제는 고정 간격이 낫고, 외부 서비스 과부하는 지수 증가가 낫습니다.
여기서는 실패 원인이 대개 "동물이 아직 없음" 또는 "DB 일시 오류"라 고정 간격이 적절합니다.

### 5-6. Recoverer가 보상 이벤트를 Outbox로 발행한다

```java
(record, ex) -> {
    try {
        Map<String, Object> payload = (Map<String, Object>) record.value();
        String eventType = (String) payload.get("eventType");
        ...
        switch (eventType) {
            case "FAVORITE_ADDED":
                FavoriteCompensationEvent compensationEvent =
                        FavoriteCompensationEvent.forAddedFailure(eventId, userId, animalId,
                                "animal-service failed to increment favoriteCount after 3 retries...");
                // ★ Outbox 패턴으로 보상 이벤트 발행
                outboxService.saveEvent(
                        "FavoriteCompensation", userId.toString(),
                        "FAVORITE_COMPENSATION_REQUIRED",
                        "user.compensation.events", compensationEvent);
                break;
```

**여기서 다시 Outbox를 쓴 것이 중요합니다.** 보상 이벤트를 `kafkaTemplate.send()`로 직접 보내면,
전송이 실패하면 보상이 사라집니다. **보상은 마지막 방어선이므로 가장 확실한 방법으로 보내야 합니다.**
Outbox 테이블에 쓰면 Debezium이 반드시 전달합니다.

즉 이 프로젝트에는 **두 방향의 Outbox**가 있습니다.

```
user-service  ──[user.favorite.events]──►  animal-service     (정방향)
user-service  ◄──[user.compensation.events]──  animal-service (역방향/보상)
```

### 5-7. FAVORITE_REMOVED는 보상하지 않는다 — 판단의 근거

```java
case "FAVORITE_REMOVED":
    // FAVORITE_REMOVED 실패 → 보상 불필요
    // 이유: user-service는 이미 favorite 삭제 완료 (사용자 의도 달성)
    //       animal-service의 favoriteCount 불일치는 Eventually Consistent로 처리
    //       (정기 배치 또는 다음 찜 추가 시 자연스럽게 맞춰짐)
    log.warn("[RECOVERER] FAVORITE_REMOVED failed, but compensation NOT needed. ...");
    break;
```

**"모든 실패를 보상해야 하는 건 아니다"** 는 판단입니다.

| 상황 | 사용자가 느끼는 것 | 보상 필요? |
|---|---|---|
| 찜 추가 실패 | 찜 목록엔 있는데 카운트가 안 올라감 | ⭕ 찜을 취소해 일관성 회복 |
| 찜 취소 실패 | 찜은 취소됐고 카운트만 1 크다 | ❌ 사용자 목적은 달성됨. 숫자만 살짝 부정확 |

찜 취소를 "보상"하려면 **찜을 다시 만들어야** 하는데, 그건 사용자 의도를 거스릅니다.
"숫자가 1 크다"와 "취소했는데 다시 찜이 됐다" 중 어느 쪽이 나쁜지 명확합니다.

**비즈니스 영향으로 기술적 결정을 정당화**한 좋은 예이고, 그 근거를 주석에 남긴 것도 좋습니다.

⚠️ 다만 *"정기 배치 또는 다음 찜 추가 시 자연스럽게 맞춰짐"* 은 **아직 사실이 아닙니다.**
카운트를 재계산하는 배치가 이 레포에 없고, 찜 추가는 `favoriteCount++`이라 어긋난 값을 교정하지 않습니다.
`favoriteCount`를 주기적으로 실제 개수로 맞추는 배치가 필요합니다
(user-service의 `favorites` 테이블을 세어야 하므로, animal-service가 직접은 못 하고
user-service가 집계를 이벤트로 보내는 형태가 됩니다).

### 5-8. FATAL-ERROR 처리 — "보상마저 실패했을 때"

```java
} catch (Exception compensationEx) {
    log.error("""
                    ╔══════════════════════════════════════════════════════════════════╗
                    ║                         🚨 FATAL-ERROR 🚨                        ║
                    ║          Compensation transaction failed to save!                ║
                    ║              Manual intervention required!                       ║
                    ╚══════════════════════════════════════════════════════════════════╝
                    Topic: {} / Partition: {} / Offset: {}
                    Original Payload: {}
                    Action Required:
                    1. Check user-service database for inconsistent favorite records
                    2. Manually publish compensation event or fix data
                    3. Investigate why OutboxService.saveEvent() failed
                    4. Monitor for cascading failures
                    """, ...);
    // Consumer가 멈추지 않도록 예외를 삼킴 (swallow)
}
```

**보상 이벤트 저장까지 실패한 상황**을 대비했습니다. 이때의 판단:

- **예외를 삼킨다** — Recoverer에서 예외를 던지면 그 메시지를 계속 재시도하며
  **소비자 전체가 멈춥니다**(head-of-line blocking). 뒤의 정상 메시지도 처리되지 않습니다.
  한 건의 데이터 불일치보다 서비스 정지가 더 나쁘므로 삼키는 게 맞습니다.
- **대신 로그를 눈에 띄게 만든다** — 박스 아트, 이모지, 그리고 **"무엇을 해야 하는지" 4단계 지시**까지
  적어 두었습니다. 새벽 3시에 이 로그를 보는 사람에게 필요한 게 정확히 이것입니다.
- Java 15의 **Text Block(`"""`)** 을 써서 여러 줄 로그를 읽기 좋게 작성했습니다.

> 💡 실무적으로 한 단계 더 나아가려면 **로그에서 끝나지 않아야** 합니다.
> 주석에도 *"Slack/PagerDuty 연동 권장"* 이라고 적혀 있습니다.
> 이 프로젝트에는 Prometheus + Grafana가 이미 있으니, `Counter` 메트릭을 하나 올리고
> Grafana 알림 규칙을 붙이면 됩니다.
> ```java
> meterRegistry.counter("compensation.fatal.error", "topic", record.topic()).increment();
> ```

### 5-9. Producer 설정 — 안정성 3종 세트

[config/KafkaProducerConfig.java](../animal-service/src/main/java/com/pawbridge/animalservice/config/KafkaProducerConfig.java)

```java
configProps.put(ProducerConfig.ACKS_CONFIG, "all");              // 모든 replica 확인
configProps.put(ProducerConfig.RETRIES_CONFIG, 3);               // 재시도 3회
configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // 멱등성 보장
```

**`acks` 옵션의 의미**

```
acks=0    보내고 확인 안 함        → 가장 빠름, 유실 가능
acks=1    리더만 받으면 성공        → 리더가 죽으면 유실 가능
acks=all  모든 복제본이 받아야 성공  → 가장 안전, 조금 느림  ★
```

**`enable.idempotence=true`가 재시도와 짝을 이룹니다.**

```
재시도만 있으면:
  메시지 전송 → 브로커는 받았는데 응답이 유실 → 프로듀서가 "실패"로 판단 → 재전송
  → 브로커에 같은 메시지가 2개  💀

멱등성이 켜지면:
  프로듀서가 메시지마다 시퀀스 번호를 붙임
  → 브로커가 "이미 받은 번호"를 알아보고 버림 → 정확히 한 번 ✅
```

즉 `retries`만 켜고 `idempotence`를 끄면 **재시도가 중복을 만듭니다.**
두 옵션을 함께 켠 것이 정확합니다.

### 5-10. ⚠️ 파티션 키에 관한 주석이 실제와 다릅니다

Consumer 주석에 이렇게 적혀 있습니다.

```
단일 토픽 이유:
- favoriteCount는 숫자 필드로 순서가 중요 (DB 조회로 검증 불가)
- 동일한 animalId는 동일 파티션으로 전송 → FIFO 보장   ← ★
- 찜 추가(+1) → 찜 취소(-1) 순서 보장 필수
```

**그런데 실제 파티션 키는 `animalId`가 아니라 `userId`입니다.**

```java
// user-service/FavoriteServiceImpl
outboxService.saveEvent("Favorite", userId.toString(), "FAVORITE_ADDED", ...);
//                                  └── aggregateId = userId
```
```json
// infrastructure/kafka/connectors/user-outbox-connector.json
"transforms.outbox.table.field.event.key": "aggregate_id"
```

`aggregate_id`(= userId)가 Kafka 메시지 키가 되고, 키가 같으면 같은 파티션으로 갑니다.

**실제 보장되는 것과 안 되는 것**

| 시나리오 | 순서 보장? |
|---|---|
| 같은 사용자가 3번 동물 찜 → 취소 | ✅ 키(userId)가 같아 같은 파티션 |
| 사용자 A와 B가 같은 3번 동물을 동시에 찜 | ❌ 키가 달라 다른 파티션 가능 |

두 번째 경우가 문제가 될 수 있습니다. 카운트 증가가 **읽고-더하고-쓰기**이기 때문입니다.

```java
// AnimalCommandService
@Transactional
public void incrementFavoriteCount(Long id) {
    Animal animal = animalRepository.findById(id).orElseThrow(...);   // 읽기
    animal.incrementFavoriteCount();                                  // favoriteCount++
}                                                                     // 커밋 시 UPDATE
```

두 스레드가 동시에 실행하면 **갱신 유실(lost update)** 이 발생합니다.

```
스레드1: favoriteCount 읽음 = 5        스레드2: favoriteCount 읽음 = 5
스레드1: 6으로 UPDATE                  스레드2: 6으로 UPDATE
→ 찜은 2건 늘었는데 카운트는 1만 늘었다
```

**현재는 우연히 안전합니다.** `ConcurrentKafkaListenerContainerFactory`의 기본 `concurrency`가 1이라
한 인스턴스에서 한 스레드만 처리하기 때문입니다. 하지만:

- animal-service를 **2대 이상으로 늘리면** 파티션이 나뉘어 동시 처리가 시작됩니다.
- `concurrency`를 올리면 같은 일이 벌어집니다.

**고치는 방법 세 가지**

```java
// (A) 원자적 UPDATE — 가장 간단하고 확실하다
@Modifying
@Query("UPDATE Animal a SET a.favoriteCount = a.favoriteCount + 1 WHERE a.id = :id")
int incrementFavoriteCount(@Param("id") Long id);
// DB가 원자적으로 처리 → 읽기-쓰기 틈이 없다

// (B) 낙관적 락 — 충돌을 감지해 재시도
@Version private Long version;    // Animal 엔티티에 추가
// 충돌 시 OptimisticLockException → Kafka 재시도가 자동으로 처리

// (C) 파티션 키를 animalId로 바꾼다 — 주석의 의도대로
// outboxService.saveEvent("Favorite", animalId.toString(), ...)
// 단, 그러면 같은 사용자의 add/remove 순서 보장이 깨진다
```

**(A)가 정답에 가깝습니다.** `favoriteCount`는 순서가 아니라 **누적 합**이 중요한 값이고,
DB에게 덧셈을 맡기면 순서·동시성 문제가 함께 사라집니다.
그러면 `decrementFavoriteCount`의 `if (favoriteCount > 0)` 방어도 SQL로 옮기면 됩니다.

```java
@Query("UPDATE Animal a SET a.favoriteCount = a.favoriteCount - 1 " +
       "WHERE a.id = :id AND a.favoriteCount > 0")
```

### 5-11. 두 서비스의 소비자 설정 차이

| | user-service | animal-service |
|---|---|---|
| `TRUSTED_PACKAGES` | `"*"` ⚠️ | `"com.pawbridge.*"` ✅ |
| AckMode | `MANUAL_IMMEDIATE` | `MANUAL` |
| ErrorHandler | 없음 (직접 try-catch + ack) ❌ | `DefaultErrorHandler` + Recoverer ✅ |
| 실패 시 | ack해서 유실 ❌ | 재시도 → 보상 ✅ |
| 역직렬화 기본 타입 | — | `VALUE_DEFAULT_TYPE = java.util.Map` |

**`animal-service`의 설정을 다른 서비스에 옮기는 것이 가장 빠른 개선입니다.**
특히 `TRUSTED_PACKAGES`를 좁힌 것과 `DefaultErrorHandler`를 붙인 것은 그대로 복사할 수 있습니다.

`VALUE_DEFAULT_TYPE = java.util.Map`은 "타입 정보 헤더가 없으면 Map으로 역직렬화하라"는 뜻입니다.
Debezium이 보낸 JSON에는 자바 타입 헤더가 없으므로 이 설정이 필요합니다.
그래서 리스너 시그니처가 `Map<String, Object> payload`입니다.

---

## 6. 그 외 눈여겨볼 코드

### 6-1. `NoticeNumberGenerator` — count 기반 번호 생성의 위험

[service/NoticeNumberGenerator.java](../animal-service/src/main/java/com/pawbridge/animalservice/service/NoticeNumberGenerator.java)

```java
public String generate() {
    String prefix = "MAN-" + LocalDate.now().format(DATE_FORMAT) + "-";   // MAN-251230-
    long count = animalRepository.countByApmsNoticeNoStartingWith(prefix);
    long nextNumber = count + 1;
    return prefix + String.format("%06d", nextNumber);                    // MAN-251230-000001
}
```

APMS가 아니라 보호소가 **직접 등록**할 때 공고번호를 만들어 주는 코드입니다.
`apmsNoticeNo`가 `nullable = false, unique = true`이므로 값이 반드시 필요합니다.

⚠️ **두 가지 문제**

1. **동시성** — 두 보호소가 같은 순간에 등록하면 `count`를 똑같이 읽어 **같은 번호**를 만듭니다.
   유니크 제약 위반으로 한쪽이 500 에러를 받습니다.
2. **삭제 후 중복** — `MAN-251230-000003`을 삭제하면 count가 2로 줄어, 다음 등록이
   `MAN-251230-000003`을 다시 만듭니다. **번호가 재사용됩니다.**

**해결 방법**

```java
// (A) DB 시퀀스/AUTO_INCREMENT를 쓴다 (별도 테이블)
// (B) 마지막 번호를 조회한다 (count가 아니라 max)
@Query("SELECT MAX(a.apmsNoticeNo) FROM Animal a WHERE a.apmsNoticeNo LIKE :prefix%")
// → 삭제해도 번호가 되돌아가지 않는다. 다만 동시성은 여전히 남음
// (C) 순번을 버리고 UUID 일부나 타임스탬프를 쓴다
//     "MAN-251230-" + System.nanoTime() 뒤 6자리 등
// (D) 유니크 위반 시 재시도 루프를 감싼다 (user-service 닉네임 방식)
```

가장 견고한 건 (A)이고, 가장 빠른 건 (D)입니다.

### 6-2. `Animal` 엔티티의 비즈니스 메서드

```java
public Integer getAge() {
    if (birthYear == null) return null;
    return Year.now().getValue() - birthYear;
}

public boolean isNoticeExpiringSoon() {
    if (noticeEndDate == null) return false;
    return LocalDate.now().plusDays(3).isAfter(noticeEndDate);
}
```

**계산 로직을 엔티티 안에 둔 것은 좋습니다.** 나이를 여러 곳에서 계산하면 기준이 어긋납니다.

⚠️ 다만 `isNoticeExpiringSoon()`에 **경계 오류**가 있습니다.

```
오늘 = 1월 1일,  "3일 이내"를 판정하려고 함

noticeEndDate = 1월 3일  →  (1/1+3=1/4).isAfter(1/3) = true  ✅ 포함
noticeEndDate = 1월 4일  →  (1/4).isAfter(1/4)      = false ❌ 제외 (정확히 3일 후인데!)
noticeEndDate = 작년      →  true  ✅ ...이미 끝난 공고도 "임박"으로 잡힌다
```

`isAfter`는 "같으면 false"입니다. 결과적으로 **실질 D-2 이내**만 잡히고, **이미 지난 공고도 포함**됩니다.

```java
// 의도한 동작: 오늘 ≤ 종료일 ≤ 오늘+3
public boolean isNoticeExpiringSoon() {
    if (noticeEndDate == null) return false;
    LocalDate today = LocalDate.now();
    return !noticeEndDate.isBefore(today) && !noticeEndDate.isAfter(today.plusDays(3));
}
```

`getAge()`도 엄밀히는 "만 나이"가 아니라 "연 나이"입니다(생일 반영 안 됨).
`birthYear`만 있으니 어쩔 수 없지만, 주석의 *"만 나이"* 는 정확하지 않습니다.

### 6-3. ⚠️ `updateFromApms()`의 파라미터 17개

```java
public void updateFromApms(
        String breed, Integer birthYear, String weight, String color,
        Gender gender, NeuterStatus neuterStatus, String specialMark,
        String apmsProcessState, LocalDate noticeStartDate, LocalDate noticeEndDate,
        LocalDateTime apmsUpdatedAt, LocalDate happenDate, String happenPlace,
        String imageUrl, String imageUrl2, Shelter shelter, AnimalStatus status) { ... }
```

의도(어떤 필드만 갱신할지 명시)는 훌륭하지만 **호출부가 매우 위험합니다.**

```java
animal.updateFromApms(
        extractBreedName(...), extractBirthYear(...),
        apmsAnimal.getWeight(),      // ← String
        apmsAnimal.getColorCd(),     // ← String  ★ 이 둘을 바꿔 써도 컴파일된다
        ...);
```

`weight`와 `color`는 둘 다 `String`이라 **순서를 바꿔도 컴파일러가 못 잡습니다.**
`imageUrl`/`imageUrl2`, `noticeStartDate`/`noticeEndDate`도 같은 위험이 있습니다.
그리고 `imageUrl`과 `imageUrl2`가 뒤바뀌면 목록의 대표 사진이 전부 바뀌는데,
**테스트가 없으므로 아무도 모릅니다.**

**개선: 파라미터 객체로 묶기**

```java
// ApmsSnapshot이라는 record를 만들어 이름으로 지정하게 한다
public record ApmsSnapshot(String breed, Integer birthYear, String weight, String color, ...) {}

public void updateFromApms(ApmsSnapshot s) {
    this.breed = s.breed();
    this.weight = s.weight();
    ...
}

// 호출부 — 이름이 있으니 순서를 틀릴 수 없다
animal.updateFromApms(ApmsSnapshot.builder()
        .breed(extractBreedName(apmsAnimal.getKindNm()))
        .weight(apmsAnimal.getWeight())
        .color(apmsAnimal.getColorCd())
        ...build());
```

`createNewAnimal()`은 이미 `Animal.builder()`로 이름을 지정하고 있으므로,
**같은 방식을 업데이트에도 적용하면 일관성까지 좋아집니다.**

### 6-4. S3 업로드

[service/S3Service.java](../animal-service/src/main/java/com/pawbridge/animalservice/service/S3Service.java)

```java
private static final List<String> ALLOWED_IMAGE_TYPES = Arrays.asList(
        "image/jpeg", "image/jpg", "image/png", "image/gif", "image/webp");

public String uploadImage(MultipartFile file) {
    validateImageFile(file);
    String key = ANIMALS_FOLDER + UUID.randomUUID() + extension;   // animals/{uuid}.jpg
    ...
}
```

**파일명을 UUID로 바꾸는 이유가 세 가지입니다.**

1. **중복 방지** — 두 사람이 `dog.jpg`를 올리면 하나가 덮어씌워집니다.
2. **경로 조작 방지** — 원본 파일명에 `../../etc/passwd` 같은 값이 들어올 수 있습니다.
3. **한글·특수문자 문제 회피** — `강아지 사진(1).jpg`는 URL 인코딩 문제를 일으킵니다.

**삭제 실패를 예외로 만들지 않은 판단도 적절합니다.**

```java
} catch (Exception e) {
    log.error("이미지 삭제 실패: {}", key, e);
    // 삭제 실패는 예외를 던지지 않음 (비즈니스 로직에 영향 없음)
}
```

동물 정보를 지웠는데 S3 파일 삭제가 실패했다고 전체를 롤백하면, 사용자는 "삭제가 안 된다"고 느낍니다.
남은 파일은 스토리지 비용만 조금 쓰는 문제이므로 **로그만 남기고 넘어가는 게 맞습니다.**
(나중에 정리 배치로 고아 파일을 청소하면 됩니다.)

⚠️ **`contentType` 검증만으로는 부족합니다.** `Content-Type` 헤더는 클라이언트가 마음대로 보낼 수
있습니다. 실행 파일을 `image/png`라고 주장하며 올릴 수 있습니다.
S3에 올린 파일을 브라우저가 직접 여는 구조라면, **파일 시그니처(magic number) 검증**을 추가하는 게
안전합니다.

### 6-5. 관리자 통계 쿼리

[admin/repository/AdminStatsRepository.java](../animal-service/src/main/java/com/pawbridge/animalservice/admin/repository/AdminStatsRepository.java)

```java
@Query("SELECT new com.pawbridge.animalservice.admin.dto.DailyAnimalStatsResponse(" +
       "CAST(a.createdAt AS LocalDate), COUNT(a)) " +
       "FROM Animal a " +
       "WHERE CAST(a.createdAt AS LocalDate) BETWEEN :startDate AND :endDate " +
       "GROUP BY CAST(a.createdAt AS LocalDate) " +
       "ORDER BY CAST(a.createdAt AS LocalDate)")
List<DailyAnimalStatsResponse> countDailyAnimals(...);
```

**JPQL의 생성자 표현식(`SELECT new ...`)** 을 썼습니다. 조회 결과를 `Object[]`로 받아 손으로
DTO로 옮기는 대신, **JPA가 바로 DTO를 만들어 줍니다.** 엔티티를 통째로 가져오지 않아
필요한 두 컬럼만 조회하므로 효율적입니다.

⚠️ **성능 주의**: `WHERE CAST(a.createdAt AS LocalDate) BETWEEN ...` 는 **컬럼에 함수를 적용**하므로
`created_at` 인덱스를 사용할 수 없습니다(전체 스캔).

```sql
-- 인덱스를 쓰려면 컬럼을 그대로 두고 범위로 비교해야 한다
WHERE a.createdAt >= :startDateTime AND a.createdAt < :endDatePlusOneDay
```

`GROUP BY`에는 `CAST`가 필요하지만 `WHERE`에는 필요하지 않습니다.
데이터가 늘면 관리자 통계 화면이 눈에 띄게 느려집니다.
(user-service의 `countDailySignups`도 확인해 볼 가치가 있습니다.)

---

## 7. 이 문서에서 배울 개념 총정리

| 개념 | 한 줄 설명 | 코드 위치 |
|---|---|---|
| **Spring Batch Job/Step/Chunk** | 대량 처리를 메모리·트랜잭션·재시작 관점에서 구조화 | `batch/job` |
| **Chunk 크기 = 트랜잭션 크기** | 크면 빠르고 위험, 작으면 안전하고 느리다 | `CHUNK_SIZE=500` |
| **Tasklet vs Chunk** | 한 덩어리 작업 vs 반복 아이템 처리 | `elasticsearchIndexStep` |
| **`ItemReader`의 계약** | 한 건씩 반환, 끝나면 null → 페이징은 직접 어댑팅 | `ApmsItemReader` |
| **싱글톤 Reader의 상태 오염** | `beforeStep` 초기화 또는 `@StepScope` | `StepExecutionListener` |
| **JobParameters와 JobInstance** | 같은 파라미터는 재실행 불가 → timestamp로 회피 | `ApmsAnimalSyncScheduler` |
| **`job.enabled: false`** | 앱 시작 시 자동 실행 방지 | config repo |
| **skip vs filtered** | Processor가 null 반환하면 skip 카운트가 안 오른다 | ⚠️ 2-9 |
| **외부 데이터 정규화** | 지저분함을 경계에서 막아 도메인을 지킨다 | `AnimalItemProcessor` |
| **코드 → enum 변환** | `"417000"`의 의미를 아는 곳을 한 곳으로 | `Species.fromCode` |
| **Upsert** | 자연키로 존재 확인 후 갱신/생성 | `findByApmsDesertionNo` |
| **필드 소유권 분리** | 배치가 사용자 데이터를 덮어쓰지 않게 | `updateFromApms` |
| **외부 상태 vs 자체 상태** | `apmsProcessState`와 `status`를 분리 | `Animal` |
| **CQRS** | 쓰기는 MySQL, 읽기는 ES | `AnimalFacade` |
| **Facade** | 저장소 선택 지식을 컨트롤러에서 숨긴다 | `AnimalFacade` |
| **형태소 분석(nori)** | 한국어는 공백 분리로 검색이 안 된다 | ES Dockerfile |
| **사용자 사전** | 도메인 단어("믹스견")를 지켜낸다 | 매핑 JSON |
| **`decompound_mode: mixed`** | 원본 + 분해 토큰을 모두 색인 | 매핑 JSON |
| **품사 필터** | 조사·어미를 버려 인덱스와 품질 개선 | `nori_posfilter` |
| **Keyword vs Text** | 정확 일치·정렬·집계 vs 형태소 검색 | `AnimalDocument` |
| **비정규화** | ES엔 JOIN이 없으니 복사해 넣는다 | `shelter_name` |
| **`must` vs `filter`** | 필터 조건은 `filter`가 빠르다(점수 계산 생략+캐시) | 개선 지점 |
| **Delegator 패턴** | Kafka를 아는 층과 모르는 층 분리 → 테스트 가능 | `Consumer`/`Handler` ★ |
| **멱등성 + 트랜잭션** | 체크·처리·기록을 한 트랜잭션에 | `FavoriteEventHandler` ★ |
| **`DefaultErrorHandler`** | 재시도 정책 + Recoverer | `KafkaConsumerConfig` ★ |
| **`FixedBackOff`** | 고정 간격 재시도 (vs 지수 증가) | `KafkaConsumerConfig` |
| **보상의 선택** | 모든 실패를 보상할 필요는 없다 | `FAVORITE_REMOVED` |
| **head-of-line blocking** | Recoverer에서 예외를 던지면 소비자가 멈춘다 | FATAL-ERROR |
| **`acks=all` + 멱등 프로듀서** | 재시도만 켜면 중복이 생긴다 | `KafkaProducerConfig` |
| **파티션 키와 순서** | 키가 같아야 순서가 보장된다 | ⚠️ 5-10 |
| **lost update** | 읽고-더하고-쓰기는 원자적 UPDATE로 | ⚠️ 5-10 |
| **함수 적용과 인덱스** | `WHERE CAST(col)`은 인덱스를 못 쓴다 | ⚠️ 6-5 |

---

## 8. 개선 우선순위

| 순위 | 항목 | 심각도 | 근거 |
|---|---|---|---|
| 1 | Reader가 API 실패를 삼켜 배치가 "성공"한다 | **높음** | 데이터가 조용히 누락됨 (2-5) |
| 2 | `.skip(Exception.class)` + Processor의 null 반환 | **높음** | 실패가 집계되지 않아 알 수 없다 (2-9) |
| 3 | `favoriteCount`를 원자적 UPDATE로 변경 | 중간 | 인스턴스를 늘리는 순간 갱신 유실 (5-10) |
| 4 | `NoticeNumberGenerator`의 count 기반 순번 | 중간 | 동시 등록 시 충돌, 삭제 후 번호 재사용 (6-1) |
| 5 | 배치 실패 시 알림 (Job 상태 확인) | 중간 | 지금은 로그만 (2-5) |
| 6 | `updateFromApms`를 파라미터 객체로 | 중간 | 같은 타입 인자 교차 위험, 테스트 없음 (6-3) |
| 7 | `favoriteCount` 정합성 교정 배치 | 중간 | 주석의 "eventually consistent"가 아직 사실이 아님 (5-7) |
| 8 | Processor의 건당 SELECT 2회 → 일괄 조회 | 낮음 | 5만 건 = 10만 쿼리 (2-12) |
| 9 | ES 조회 메서드의 `@Transactional` 제거 | 낮음 | 불필요한 DB 커넥션 점유 (3절) |
| 10 | `must` → `filter` 전환 | 낮음 | 캐시·점수 생략으로 성능 개선 (4-5) |
| 11 | 통계 쿼리의 `WHERE CAST(...)` 제거 | 낮음 | 인덱스 미사용 (6-5) |
| 12 | `isNoticeExpiringSoon()` 경계 수정 | 낮음 | D-3이 실제로는 D-2 (6-2) |
| 13 | S3 업로드에 파일 시그니처 검증 추가 | 낮음 | contentType은 위조 가능 (6-4) |

---

**다음 문서** → [05-community-service.md](05-community-service.md) — 게시글·댓글, 그리고 세 번째 Outbox 구현
