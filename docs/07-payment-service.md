# 07. payment-service — 가장 작은 서비스, 가장 큰 위험

> 자바 파일 18개. 이 레포에서 가장 작습니다. 그런데 **돈을 다루는 유일한 서비스**입니다.
> 코드에는 "🛡️ 안전장치 1", "🛡️ 안전장치 2" 같은 주석이 달려 있고, 실제로 결제 시스템에서
> 반드시 필요한 방어가 여러 개 들어 있습니다.
>
> 동시에 **정적으로 확인되는 결함이 세 개** 있습니다. 하나는 결제 자체를 막고,
> 하나는 보상 이벤트를 지워버리며, 하나는 타임스탬프를 비웁니다.

```
payment-service/src/main/java/com/pawbridge/paymentservice/
├── client/
│   ├── TossPaymentsClient.java     ★ 외부 PG(토스) 호출
│   └── StoreServiceClient.java     ★ 금액 교차 검증용
├── common/entity/
│   ├── BaseEntity.java             createdAt / updatedAt
│   └── Outbox.java
├── domain/payment/
│   ├── controller/PaymentController.java   엔드포인트 1개뿐
│   ├── dto/         Toss 요청·응답 DTO
│   ├── entity/      Payment, PaymentStatus
│   ├── repository/
│   └── service/PaymentServiceImpl.java     ★ 이 파일이 사실상 전부
└── PaymentServiceApplication.java
```

---

## 1. 결제가 흐르는 전체 경로

먼저 그림을 잡아야 코드가 읽힙니다.

```
[브라우저]
   ① 결제하기 클릭
   ② 토스 결제창(SDK)이 열림 → 카드 정보 입력 → 토스가 인증
   ③ 토스가 브라우저로 paymentKey, orderId, amount 를 돌려줌
        ★ 이 시점에 아직 결제는 "승인"되지 않았다. 인증만 됐다.
   ④ 브라우저 → 우리 서버:  POST /api/payments/confirm { paymentKey, orderId, amount }
                                    │
[payment-service]                   ▼
   ⑤ 이미 처리한 주문인가?               (안전장치 1: 멱등성)
   ⑥ store-service에 실제 결제 금액을 물어봄 (안전장치 2: 금액 위조 방지)
   ⑦ 토스 API 호출 → 실제 승인 (돈이 실제로 빠져나가는 순간)
   ⑧ payments 테이블 저장 + outbox 저장  (같은 트랜잭션)
        │
        ▼ Debezium → Kafka "payment.events"
[store-service]
   ⑨ PaymentEventConsumer 수신 → 주문 상태를 PAID로
```

### ③④가 나뉘어 있는 이유 — 결제의 핵심 보안 구조

**"인증"과 "승인"이 분리되어 있습니다.**

```
인증(authentication): 카드 소유자가 맞는지 확인. 토스와 브라우저 사이에서 일어난다.
승인(confirm):        실제로 금액을 청구한다. 반드시 서버가 시크릿 키로 호출해야 한다.
```

**왜 브라우저가 직접 승인하지 못하게 하나** — 승인 요청에는 **시크릿 키**가 필요합니다.
브라우저에 시크릿 키를 두면 누구나 임의 금액을 승인시킬 수 있습니다.
그래서 브라우저는 "인증됐다"는 증표(`paymentKey`)만 받고, 서버가 그걸 시크릿 키와 함께
토스에 보내 승인합니다.

**이 구조 덕분에 서버는 "금액을 검증할 마지막 기회"를 갖습니다.** 그게 안전장치 2입니다.

---

## 2. 🛡️ 안전장치 2 — 금액 위조 방지 (가장 중요한 코드)

[domain/payment/service/PaymentServiceImpl.java](../payment-service/src/main/java/com/pawbridge/paymentservice/domain/payment/service/PaymentServiceImpl.java)

```java
// 🛡️ 안전장치 2: 이중 검증 (스토어 서비스와 교차 검증)
StoreOrderResponse orderInfo = storeServiceClient.getOrder(orderId);
if (!orderInfo.getTotalAmount().equals(amount)) {
    log.error("Payment Verification Failed! Request: {}, Real: {}", amount, orderInfo.getTotalAmount());
    throw new IllegalStateException("Payment Amount Mismatch");
}
```

**이 6줄이 이 서비스에서 가장 중요합니다.**

### 왜 필요한가 — 공격 시나리오

```
① 사용자가 38,000원 상품을 주문 → 주문 UUID 생성
② 브라우저에서 결제창을 열 때 amount 를 조작한다 (개발자 도구로 100원으로 변경)
③ 토스는 100원으로 인증한다 (토스는 "우리 상품이 얼마인지" 모른다)
④ 브라우저 → 우리 서버: { orderId: "abc", amount: 100 }

검증이 없으면:
   토스에 100원 승인 → 성공 → payments 에 100원 기록
   → store-service는 "결제 완료" 이벤트를 받고 주문을 PAID로 바꾼다
   → 38,000원 상품을 100원에 샀다  💀
```

**핵심은 "클라이언트가 보낸 금액을 절대 믿지 않는다"** 입니다.
서버가 **주문의 원본(store-service)** 에 다시 물어봐서 대조합니다.

> 📌 이것은 결제 연동에서 가장 흔한 실제 사고이고, 실무 코드 리뷰에서 가장 먼저 확인하는 항목입니다.
> 이 프로젝트가 이걸 구현해 둔 것은 확실히 잘한 부분입니다.

### 같은 원칙이 다른 곳에도 적용되어 있다

[03-user-service.md](03-user-service.md#1-회원가입--순서가-곧-규칙이다)의
`if (requestDto.role() == Role.ROLE_ADMIN) throw ...` 도 같은 원칙입니다.

> **클라이언트에서 오는 값 중 권한·금액·소유자에 해당하는 것은 서버가 반드시 재검증한다.**

### ⚠️ 그런데 이 검증 호출이 404를 맞습니다

```java
// payment-service/client/StoreServiceClient.java
@FeignClient(name = "store-service", url = "${service.store.url:}")
public interface StoreServiceClient {
    @GetMapping("/api/orders/uuid/{orderUuid}")        // ← 요청 경로
    StoreOrderResponse getOrder(@PathVariable("orderUuid") String orderUuid);
}
```

```java
// store-service/domain/order/controller/OrderController.java
@RequestMapping("/api/v1/orders")                       // ← 실제 경로
public class OrderController {
    @GetMapping("/uuid/{orderUuid}")
    ...
}
// 실제 전체 경로: /api/v1/orders/uuid/{orderUuid}
```

**`/api/orders/...` 와 `/api/v1/orders/...` 가 어긋납니다.**

store-service의 컨트롤러를 전부 확인했지만 `/api/orders`로 매핑된 것은 없습니다
(모두 `/api/v1/` 접두사입니다). 즉 **Feign 호출은 404를 받습니다.**

```java
StoreOrderResponse orderInfo = storeServiceClient.getOrder(orderId);   // try-catch 없음
```

`FeignException.NotFound`가 그대로 밖으로 나가므로 **`confirmPayment` 전체가 500으로 실패**하고,
토스 승인 호출에는 도달하지 못합니다.

**원인은 [02-api-gateway.md에서 본 경로 이중 체계](02-api-gateway.md#6-2-인가-목록과-경로-리라이트가-엇갈려서-생긴-실제-구멍)입니다.**
프론트엔드는 `/api/orders`로 부르고 게이트웨이가 `/api/v1/orders`로 바꿔줍니다.
**그런데 서비스 간 Feign 호출은 게이트웨이를 거치지 않습니다.**
프론트 기준 경로를 그대로 적으면 어긋납니다.

**고치는 방법**

```java
// (A) 실제 서비스 경로를 쓴다 — 정답
@GetMapping("/api/v1/orders/uuid/{orderUuid}")

// (B) store-service에 내부 전용 엔드포인트를 만든다 — 더 명확
//     결제 검증은 "내부 호출"이므로 mypage 처럼 별도 경로로 두면 의도가 드러난다
@GetMapping("/api/v1/internal/orders/{orderUuid}/amount")
```

그리고 **호출 실패를 구분해서 처리해야 합니다.**

```java
StoreOrderResponse orderInfo;
try {
    orderInfo = storeServiceClient.getOrder(orderId);
} catch (FeignException.NotFound e) {
    throw new IllegalStateException("존재하지 않는 주문입니다: " + orderId, e);   // 400대
} catch (Exception e) {
    throw new PaymentVerificationUnavailableException("주문 확인 불가", e);      // 503
}
if (!orderInfo.getTotalAmount().equals(amount)) { ... }
```

지금은 **"주문이 없다"와 "store-service가 죽었다"가 똑같이 500**입니다.
결제에서는 이 구분이 중요합니다. 전자는 재시도해도 안 되고, 후자는 잠시 후 재시도해야 합니다.

> 참고로 `url = "${service.store.url:}"`의 기본값이 빈 문자열이라, 값이 없으면 Feign이
> **Eureka 이름(`store-service`)으로 해석**합니다. 배포 워크플로우는 `STORE_SERVICE_URL` 이라는
> 환경변수를 심는데, `service.store.url`에 대응하는 이름은 `SERVICE_STORE_URL`이라
> **바인딩되지 않습니다.** 결과적으로 Eureka 경로로 동작하지만, 의도한 설정은 아닌 것으로 보입니다.

---

## 3. 🛡️ 안전장치 1 — 멱등성 (같은 결제를 두 번 승인하지 않기)

```java
// 🛡️ 안전장치 1: 멱등성 (Idempotency)
Optional<Payment> existingPayment = paymentRepository.findByOrderId(orderId);
if (existingPayment.isPresent()) {
    Payment payment = existingPayment.get();
    if (payment.getStatus() == PaymentStatus.DONE) {
        log.info("Payment already processed for orderId: {}", orderId);
        if (!payment.getAmount().equals(amount)) {
            throw new IllegalStateException("Payment exists but amount mismatch");
        }
        return TossPaymentResponse.builder()
                .paymentKey(payment.getPaymentKey())
                .status("DONE")
                ...build();          // ★ 성공했던 결과를 다시 돌려준다
    }
}
```

### 왜 필요한가

```
사용자가 결제 완료 버튼을 두 번 누른다
네트워크가 느려서 브라우저가 요청을 재시도한다
프론트엔드 상태 관리 버그로 confirm 이 두 번 호출된다

→ 방어가 없으면 토스에 두 번 승인 요청 → 두 번 청구될 수 있다 💀
```

**해법: "이미 처리했으면, 처리하지 않고 지난번 결과를 반환한다."**

에러를 던지는 것보다 이 방식이 낫습니다. 사용자는 결제가 이미 됐는데
"결제 실패" 화면을 보면 다시 시도하게 되고, 상황이 더 나빠집니다.
**멱등한 API는 같은 요청에 같은 응답을 줘야 합니다.**

**금액까지 대조하는 것도 정확합니다.**

```java
if (!payment.getAmount().equals(amount)) {
    throw new IllegalStateException("Payment exists but amount mismatch");
}
```

같은 `orderId`로 다른 금액이 들어온다면 정상적인 재시도가 아닙니다. 공격이거나 심각한 버그이므로
막아야 합니다.

### 토스 쪽 중복도 처리한다

```java
} catch (Exception e) {
    String errorMsg = e.getMessage();
    if (errorMsg != null && (errorMsg.contains("S008") || errorMsg.contains("ALREADY_PROCESSED_PAYMENT"))) {
        log.info("Duplicate Request detected ([S008]). Retrieving existing payment for idempotency.");
        Optional<Payment> foundPayment = paymentRepository.findByOrderId(request.getOrderId());
        ...
        return TossPaymentResponse.builder().status("DONE")...build();
    }
```

**우리 DB에는 기록이 없는데 토스에는 이미 승인된** 경우입니다.

```
① 토스 승인 성공
② 우리 DB 저장 직전에 서버가 죽음      ← 이 틈
③ 사용자가 재시도
④ 토스: "이미 처리된 결제입니다 (S008)"
```

이때 실패로 처리하면 **돈은 빠져나갔는데 주문은 안 되는** 최악의 상태가 됩니다.
그래서 S008을 "성공"으로 해석합니다. **결제 시스템에서 반드시 필요한 처리이고, 알고 넣은 코드입니다.**

### ⚠️ 문제 1 — 에러 메시지 문자열로 판단한다

```java
if (errorMsg != null && errorMsg.contains("S008"))
```

`e.getMessage()`는 Feign이 만든 문자열이고 **형식이 보장되지 않습니다.**
Feign 버전이 올라가거나 설정이 바뀌면 메시지 모양이 달라져 이 분기가 조용히 동작하지 않게 됩니다.
그러면 중복 결제가 "진짜 실패"로 처리되어 **결제 취소 + 재고 복구**가 실행됩니다.

**개선: Feign `ErrorDecoder`로 토스의 응답 본문을 파싱합니다.**

```java
@Component
public class TossErrorDecoder implements ErrorDecoder {
    @Override
    public Exception decode(String methodKey, Response response) {
        TossError body = parse(response.body());          // { "code": "S008", "message": "..." }
        return new TossApiException(body.code(), body.message(), response.status());
    }
}

// 사용
} catch (TossApiException e) {
    if ("ALREADY_PROCESSED_PAYMENT".equals(e.getCode())) { ... }   // 문자열 검색이 아니라 코드 비교
}
```

토스는 에러 코드를 응답 본문의 `code` 필드로 명확히 줍니다. 그걸 쓰는 게 맞습니다.

### ⚠️ 문제 2 — `getApprovedAt()`가 null이면 NPE

```java
.approvedAt(payment.getApprovedAt().atOffset(ZoneOffset.of("+09:00")))
```

`approvedAt`은 `Payment` 엔티티에서 nullable입니다(`approve()`를 호출해야 채워짐).
`status == DONE`인 행은 항상 `approvedAt`이 있으니 지금은 안전하지만,
**데이터를 손으로 고치거나 새 경로가 추가되면 NPE가 납니다.**
그리고 그 NPE는 **결제 재시도 경로에서** 터지므로 사용자가 결제를 못 합니다.

```java
.approvedAt(payment.getApprovedAt() == null ? null
        : payment.getApprovedAt().atOffset(ZoneOffset.of("+09:00")))
```

`ZoneOffset.of("+09:00")`을 문자열로 하드코딩한 것도 상수로 뽑는 게 좋습니다
(`private static final ZoneOffset KST = ZoneOffset.of("+09:00")`).

---

## 4. 🚨 보상 트랜잭션이 스스로를 지운다

이 서비스에서 가장 심각한 결함입니다. 두 개의 실패 경로를 나란히 놓고 보면 명확합니다.

### 경로 A — 토스 API 호출 실패 (올바르게 처리됨)

```java
// [진짜 실패] 잔액 부족, 네트워크 오류 등 -> 재고 복구 필요
// 트랜잭션을 커밋시키기 위해 예외를 던지지 않고 'ABORTED' 응답을 반환함.
try {
    savePaymentFailureAndOutbox(request.getPaymentKey(), request.getOrderId(), "PAYMENT_FAILED");
} catch (JsonProcessingException ex) {
    log.error("Failed to save failure outbox during API Error", ex);
}
return TossPaymentResponse.builder().status("ABORTED")...build();     // ★ throw 하지 않는다
```

**주석이 핵심을 정확히 설명합니다: "트랜잭션을 커밋시키기 위해 예외를 던지지 않고".**

`@Transactional` 메서드에서 예외를 던지면 **그 트랜잭션 안의 모든 DB 작업이 롤백**됩니다.
outbox에 저장한 실패 이벤트도 함께 사라집니다. 그래서 **일부러 예외를 던지지 않고 반환**해
outbox 행이 커밋되게 만들었습니다.

그 결과 실패 이벤트가 Kafka로 나가고, store-service가 주문을 취소하고 재고를 복구합니다.
**정확한 판단입니다.**

### 경로 B — 토스는 성공했는데 DB 저장 실패 (🚨 잘못 처리됨)

```java
try {
    savePaymentAndOutbox(userId, response);
} catch (Exception e) {
    log.error("DB Save Failed after Toss Payment! Triggering Compensation...", e);

    // 1. 보상 트랜잭션 (결제 취소)
    cancelPayment(response.getPaymentKey(), "System Error during saving payment record");

    // 2. 실패 이벤트 발행 (재고 복구용)
    try {
        savePaymentFailureAndOutbox(response.getPaymentKey(), response.getOrderId(), "PAYMENT_FAILED");
    } catch (JsonProcessingException ex) {
        log.error("Failed to save failure outbox", ex);
    }

    throw new RuntimeException("Payment processed but failed to save record. Payment Cancelled.", e);
    //  ★ 🚨 여기서 throw 하면 위의 outbox INSERT 도 함께 롤백된다
}
```

**경로 A에서 이해한 것을 경로 B에서 잊었습니다.**

```
① 토스 승인 성공 (외부 시스템 — 롤백되지 않음)
② DB 저장 실패
③ 토스 취소 호출 → 돈은 돌려줌 (외부 시스템 — 롤백되지 않음)  ✅
④ outbox 에 PAYMENT_FAILED INSERT
⑤ throw → 트랜잭션 ROLLBACK → ④가 사라진다  💀
⑥ store-service는 아무 이벤트도 받지 못한다
   → 주문은 영원히 PENDING
   → 재고는 차감된 채로 남는다  (돈은 돌려줬는데 재고는 안 돌아온다)
```

**정리하면**: 돈은 정상적으로 환불되지만, **재고가 영구히 묶입니다.**
그 상품은 팔 수 없는 상태로 남고, 아무도 모릅니다.

#### 고치는 방법

**(A) 실패 이벤트를 별도 트랜잭션으로 커밋시킨다**

```java
// 별도 빈으로 분리 (같은 클래스 내부 호출은 프록시를 안 타서 REQUIRES_NEW 가 동작하지 않는다)
@Service
@RequiredArgsConstructor
public class PaymentFailureRecorder {
    private final OutboxRepository outboxRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)   // ★ 부모가 롤백돼도 이건 커밋
    public void record(String paymentKey, String orderId, String eventType) { ... }
}
```

> 흥미롭게도 이건 [03-user-service.md에서 문제였던 `REQUIRES_NEW`](03-user-service.md#8-5--여기에-결함이-있습니다--requires_new가-원자성을-깬다)가
> **여기서는 정답**인 경우입니다. 차이는 목적입니다.
> - 찜하기: 이벤트가 비즈니스 변경과 **함께** 커밋돼야 한다 → `REQUIRES_NEW`는 틀림
> - 실패 기록: 비즈니스 변경이 **롤백되어도** 남아야 한다 → `REQUIRES_NEW`가 맞음
>
> **트랜잭션 전파는 "무엇과 운명을 같이해야 하는가"로 결정합니다.**

**(B) 트랜잭션을 커밋한 뒤에 처리한다 — 구조적으로 더 깔끔**

```java
// confirmPayment 에서 @Transactional 을 떼고, 짧은 트랜잭션 메서드를 따로 둔다
public TossPaymentResponse confirmPayment(Long userId, TossPaymentConfirmRequest request) {
    verifyAmount(request);                                    // 트랜잭션 없음
    TossPaymentResponse response = callToss(request);         // 트랜잭션 없음 (외부 호출)
    try {
        paymentTxService.saveSuccess(userId, response);        // 짧은 트랜잭션
    } catch (Exception e) {
        cancelPayment(response.getPaymentKey(), "...");        // 외부 호출
        paymentTxService.saveFailure(response);                // 독립된 짧은 트랜잭션
        throw new PaymentSaveFailedException(...);
    }
    return response;
}
```

이 구조가 다음 절의 문제까지 함께 해결합니다.

---

## 5. ⚠️ 트랜잭션이 외부 HTTP 호출을 감싸고 있다

```java
@Transactional                                                    // ← 트랜잭션 시작
public TossPaymentResponse confirmPayment(Long userId, TossPaymentConfirmRequest request) {
    paymentRepository.findByOrderId(orderId);                       // DB
    storeServiceClient.getOrder(orderId);                          // ← HTTP (다른 서비스)
    tossPaymentsClient.confirmPayment(authorization, request);      // ← HTTP (외부 PG, 느림)
    savePaymentAndOutbox(userId, response);                        // DB
}                                                                  // ← 트랜잭션 커밋
```

**DB 커넥션을 쥔 채로 외부 HTTP 호출을 두 번 합니다.**

```
토스 API 응답이 3초 걸리면 → DB 커넥션을 3초 동안 붙잡고 있는다
동시 결제 20건 → 커넥션 20개 점유
기본 커넥션 풀(HikariCP)이 10개 → 11번째 요청부터 커넥션 대기
→ 결제뿐 아니라 그 서비스의 모든 DB 작업이 멈춘다  💀
```

토스가 일시적으로 느려지면 **payment-service 전체가 마비**됩니다.
결제 트래픽이 몰리는 시간대에 정확히 그런 일이 생깁니다.

> 📌 **원칙: 트랜잭션 안에서 외부 시스템을 호출하지 않는다.**
> DB 트랜잭션은 "짧고 순수하게" 유지하고, 네트워크 호출은 그 바깥에 둡니다.

4절의 **(B) 방식**이 이 문제를 함께 해결합니다. 외부 호출을 트랜잭션 밖으로 빼면
커넥션은 "저장하는 짧은 순간"만 점유됩니다.

---

## 6. ⚠️ `@EnableJpaAuditing`이 없다 — 타임스탬프가 전부 null

```java
// common/entity/BaseEntity.java
@MappedSuperclass
@EntityListeners(AuditingEntityListener.class)
public abstract class BaseEntity {
    @CreatedDate      @Column(updatable = false) private LocalDateTime createdAt;
    @LastModifiedDate                            private LocalDateTime updatedAt;
}
```

`Payment extends BaseEntity`이므로 결제 기록에 생성/수정 시각이 자동으로 들어갈 것처럼 보입니다.

**그런데 `@EnableJpaAuditing`이 어디에도 없습니다.**

다른 서비스는 모두 켜 두었습니다.

| 서비스 | `@EnableJpaAuditing` 위치 |
|---|---|
| user-service | `UserServiceApplication.java` ✅ |
| animal-service | `config/JpaConfig.java` ✅ |
| community-service | `config/JpaConfig.java` ✅ |
| store-service | `common/config/JpaConfig.java` ✅ |
| **payment-service** | **없음** ❌ (config 패키지 자체가 없음) |

**결과: `payments` 테이블의 `created_at`, `updated_at`이 항상 `NULL`입니다.**

- **결제가 언제 발생했는지 DB에서 알 수 없습니다.** 정산·분쟁·감사에서 가장 먼저 필요한 정보입니다.
- 조용히 실패합니다. 에러도, 경고도 없습니다.
- 다행히 `requestedAt`, `approvedAt`(토스 응답에서 받은 값)은 채워지므로 완전한 공백은 아닙니다.
  하지만 그건 **토스 기준 시각**이고, 우리 시스템에 기록된 시각은 없습니다.

**고치는 방법 — 한 줄입니다.**

```java
@EnableJpaAuditing        // ★ 추가
@EnableFeignClients
@SpringBootApplication
public class PaymentServiceApplication { ... }
```

> 💡 **여기서 배울 것**: `@EnableXxx` 계열 애노테이션은 **없으면 그 기능이 조용히 꺼집니다.**
> 이 레포에는 같은 종류의 위험이 더 있습니다 — `@EnableScheduling`이 없으면 `@Scheduled`가
> 전혀 실행되지 않고 로그도 안 남습니다. 다행히 스케줄러가 있는 세 서비스에는 모두 켜져 있습니다.
> **애노테이션 하나로 기능 전체가 사라지는 자리는 테스트로 잡는 것이 정석입니다.**

---

## 7. 실패했는데 200 OK를 반환하는 선택

```java
// Controller가 200 OK와 함께 ABORTED 상태를 반환하게 함 (프론트에서 처리 필요)
return TossPaymentResponse.builder()
        .status("ABORTED")
        .orderId(request.getOrderId())
        .paymentKey(request.getPaymentKey())
        .build();
```

```java
if (!response.getStatus().equals("DONE")) {
     log.warn("Payment status is not DONE: {}", response.getStatus());
     return response;                 // 저장하지 않고 그대로 반환
}
```

**HTTP는 200인데 본문의 `status`가 실패입니다.** REST 관례로 보면 400/402가 맞아 보입니다.

**그런데 이 선택에는 이유가 있습니다.**

```
예외를 던지면 → @Transactional 롤백 → 실패 outbox 도 사라진다 → 재고 복구 안 됨
정상 반환하면 → 커밋 → 실패 이벤트 발행 → 재고 복구됨  ✅
```

즉 **"트랜잭션을 커밋시키기 위해" 200을 반환**한 것입니다. 4절 (A)/(B)로 구조를 고치면
`402 Payment Required`를 제대로 반환하면서도 실패 이벤트를 남길 수 있습니다.

⚠️ 그리고 **`DONE`이 아닌 상태는 아무것도 기록하지 않고 반환**합니다.

```java
if (!response.getStatus().equals("DONE")) { return response; }
```

카드 결제만 쓴다면 대개 문제없지만, **가상계좌**는 승인 직후 `WAITING_FOR_DEPOSIT` 상태입니다.
그 경우 `Payment` 행이 만들어지지 않으므로, 나중에 입금 완료 웹훅이 와도 대조할 기록이 없습니다.
`PaymentStatus`에 `READY`가 정의되어 있으니 **READY 상태로 먼저 저장**해 두는 편이 확장에 유리합니다.

---

## 8. 보상 트랜잭션(결제 취소)의 마지막 방어선

```java
private void cancelPayment(String paymentKey, String reason) {
    log.warn(">>> TRIGGERING PAYMENT CANCELLATION for key: {}, reason: {}", paymentKey, reason);
    try {
        ... tossPaymentsClient.cancelPayment(authorization, paymentKey, request);
        log.info(">>> PAYMENT CANCELLED SUCCESSFULLY for key: {}", paymentKey);
    } catch (Exception e) {
        log.error(">>> CRITICAL: FAILED TO CANCEL PAYMENT during compensation! " +
                  "Manual intervention required. Key: {}", paymentKey, e);
        // In a real system, we might save this to a "Dead Letter Queue" or
        // "Failed Operations Table" for manual ops.
    }
}
```

**취소마저 실패한 경우를 대비했습니다.** 이때는 **돈을 받았는데 주문도 안 되고 환불도 안 된**
가장 나쁜 상태입니다. 사람이 개입해야 합니다.

**예외를 삼키는 것은 여기서는 맞습니다.** 취소 실패로 예외를 던지면 원래 예외(DB 저장 실패)가
가려져 원인 파악이 어려워집니다. 그리고 이미 손 쓸 방법이 없습니다.

그리고 주석이 정확히 다음 단계를 지목합니다: *"Failed Operations Table for manual ops"*.

**실제로 만들어야 하는 것**

```java
// 실패 작업 테이블 — 사람이 처리해야 할 목록
@Entity
public class FailedOperation {
    @Id @GeneratedValue private Long id;
    private String operationType;    // "TOSS_CANCEL"
    private String referenceKey;     // paymentKey
    private String reason;
    private String errorDetail;
    @Enumerated(EnumType.STRING)
    private ResolveStatus status;    // PENDING / RESOLVED
    private LocalDateTime createdAt;
}
```

이 테이블을 관리자 화면에 띄우면 "처리해야 할 사고 목록"이 됩니다.
[animal-service의 FATAL-ERROR 로그](04-animal-service.md#5-8-fatal-error-처리--보상마저-실패했을-때)와
같은 문제이고, 같은 해법이 필요합니다.

**그리고 로그만으로는 아무도 모릅니다.** Prometheus 메트릭을 하나 올리고 Grafana 알림을
붙이는 것이 이 레포에서 가장 값싼 개선입니다.

```java
meterRegistry.counter("payment.cancel.failed").increment();
// Grafana: rate(payment_cancel_failed_total[5m]) > 0 → 즉시 알림
```

---

## 9. 자잘하지만 알아둘 코드

### Basic 인증 인코딩

```java
String encodedKey = Base64.getEncoder().encodeToString((tossSecretKey + ":").getBytes(UTF_8));
String authorization = "Basic " + encodedKey;
```

**시크릿 키 뒤에 콜론(`:`)을 붙이는 것이 포인트입니다.**
HTTP Basic 인증은 `아이디:비밀번호`를 Base64로 인코딩합니다.
토스는 시크릿 키를 **아이디 자리**에 넣고 비밀번호를 비웁니다. 그래서 `키:`가 됩니다.
콜론을 빼면 인증이 실패합니다. 토스 문서 그대로 구현한 것입니다.

⚠️ 이 인코딩이 `confirmPayment`와 `cancelPayment`에서 **중복**되고, 매 요청마다 다시 계산됩니다.
값이 변하지 않으므로 한 번만 만들어 두면 됩니다.

```java
private String authorizationHeader;

@PostConstruct
void init() {
    this.authorizationHeader = "Basic " + Base64.getEncoder()
            .encodeToString((tossSecretKey + ":").getBytes(UTF_8));
}
```

더 나은 방법은 Feign `RequestInterceptor`로 옮기는 것입니다. 그러면 클라이언트 인터페이스에서
`@RequestHeader("Authorization")` 파라미터가 사라져 호출부가 깨끗해집니다.

### `Payment` 엔티티의 상태 전이

```java
public Payment(String paymentKey, ..., LocalDateTime requestedAt) {
    ...
    this.status = PaymentStatus.READY;      // ★ 생성 시 항상 READY
}
public void approve(LocalDateTime approvedAt) {
    this.status = PaymentStatus.DONE;
    this.approvedAt = approvedAt;
}
public void cancel() { this.status = PaymentStatus.CANCELED; }
```

**생성자에서 초기 상태를 강제하고, 변경은 의미 있는 이름의 메서드로만 허용합니다.**
`setStatus(DONE)`을 아무 데서나 호출할 수 없습니다.

⚠️ 다만 `cancel()`은 **아무도 호출하지 않습니다.** 결제 취소 API가 없기 때문입니다
(`PaymentController`에 `/confirm` 하나뿐). 토스 취소는 호출하는데 우리 DB의 상태는 갱신하지 않으므로,
**보상으로 취소된 결제가 DB에는 `DONE`으로 남습니다.** 정산 시 불일치가 생깁니다.

```java
// cancelPayment 성공 후 상태를 반영해야 한다
paymentRepository.findByPaymentKey(paymentKey).ifPresent(Payment::cancel);
```

`PaymentStatus.ABORTED`도 정의만 되어 있고 쓰이지 않습니다.

### Outbox에 `eventId`가 없다 (store-service와 동일)

```java
@Table(name = "outbox")
public class Outbox {
    @Id @GeneratedValue private Long id;      // ← 이게 event id
    private String aggregateType;              // "PAYMENT"
    private String aggregateId;                // paymentKey
    private String eventType;                  // "PAYMENT_COMPLETED" / "PAYMENT_FAILED"
    private String payload;
    private LocalDateTime createdAt;
}
```

```json
// payment-outbox-connector.json
"transforms.outbox.table.field.event.id": "id",
"transforms.outbox.route.topic.replacement": "payment.events"
```

**`aggregateId`가 `paymentKey`인 점이 중요합니다.** 이 값이 Kafka 메시지 키가 되므로
**같은 결제의 이벤트는 같은 파티션으로 가고 순서가 보장**됩니다.
`PAYMENT_COMPLETED` 다음에 `PAYMENT_FAILED`가 오는 순서가 뒤집히지 않습니다.

⚠️ 다만 실패 경로에서 `paymentKey`가 null일 수 있어 이렇게 방어했습니다.

```java
.aggregateId(paymentKey != null ? paymentKey : "UNKNOWN_" + orderId)
```

`aggregateId`를 `orderId`(주문 UUID)로 통일하는 편이 더 자연스럽습니다.
소비자(store-service)가 실제로 쓰는 키가 `orderId`이고, null도 되지 않습니다.

### 토픽 이름 확인 — store-service가 두 개를 구독하는 이유

```json
"topic.prefix": "payment",
"transforms.outbox.route.topic.replacement": "payment.events"
```

**실제 데이터 토픽은 `payment.events` 하나입니다.**
`payment`는 `topic.prefix`이고, Debezium이 내부 메타데이터 토픽 이름을 만들 때 쓰는 접두사입니다
(예: `payment.pawbridge_payment.outbox`). **비즈니스 이벤트가 오지 않습니다.**

```java
// store-service — payment 토픽 구독은 불필요하다
@KafkaListener(topics = {"payment.events", "payment"}, ...)
```

`"payment"`를 지워도 동작이 같습니다. [06-store-service.md 7-4](06-store-service.md#7-4-토픽을-두-개-구독하는-이유)에서
지적한 대로, 확신이 없어 둘 다 걸어둔 것으로 보입니다.

### 커넥터 이름이 `-v5`

```json
"name": "payment-outbox-connector-v5",
"database.server.id": "2002",
```

store는 `-v3`, payment는 `-v5`입니다. **설정을 여러 번 고쳐가며 새 커넥터를 만든 흔적**입니다.
Debezium은 `topic.prefix`나 `name`을 바꾸면 오프셋이 초기화되므로 실무에서도 흔한 일이지만,
**옛 커넥터가 Kafka Connect에 남아 있으면 같은 테이블을 두 커넥터가 읽어 이벤트가 중복 발행**됩니다.
배포 시 옛 커넥터를 삭제하는 절차가 필요합니다.

### 컨트롤러가 `ResponseDTO`를 쓰지 않는다

```java
@PostMapping("/confirm")
public ResponseEntity<TossPaymentResponse> confirmPayment(...) {
    return ResponseEntity.ok(response);          // ← 공통 껍데기 없음
}
```

다른 서비스는 모두 [`ResponseDTO<T>`로 감쌉니다](03-user-service.md#응답-포맷-통일).
payment-service만 토스 응답을 그대로 내보냅니다.

**프론트엔드가 이 API만 다르게 처리해야 합니다.** 그리고 전역 예외 핸들러
(`GlobalExceptionRestAdvice`)도 없어서, 예외가 나면 **Spring 기본 에러 형식**이 나갑니다.

```json
// 다른 서비스의 에러                    // payment-service의 에러
{ "code": 400,                          { "timestamp": "...",
  "message": "...",                       "status": 500,
  "data": null }                          "error": "Internal Server Error",
                                          "path": "/api/payments/confirm" }
```

결제 실패는 프론트가 반드시 다뤄야 하는 상황인데, **형식이 달라 처리가 어렵습니다.**
공통 규약을 옮겨오는 것이 좋습니다.

---

## 10. 이 문서에서 배울 개념 정리

| 개념 | 한 줄 설명 | 코드 위치 |
|---|---|---|
| **인증과 승인의 분리** | 브라우저는 증표만, 승인은 서버가 시크릿 키로 | 1절 |
| **금액 교차 검증** | 클라이언트가 보낸 금액을 절대 믿지 않는다 | 2절 ★ |
| **멱등한 결제 API** | 중복 요청에 에러가 아니라 지난 결과를 반환 | 3절 ★ |
| **PG 중복 코드 처리(S008)** | 토스엔 있고 우리 DB엔 없는 상태를 성공으로 해석 | 3절 |
| **에러 코드 vs 메시지 문자열** | `ErrorDecoder`로 구조화해서 판단한다 | ⚠️ 3절 |
| **🚨 롤백이 보상 기록을 지운다** | `throw` 하면 같은 트랜잭션의 outbox도 사라진다 | 4절 ★ |
| **`REQUIRES_NEW`의 정당한 용도** | "부모가 롤백돼도 남아야 하는 기록" | 4절 (A) |
| **트랜잭션 안의 외부 호출** | DB 커넥션을 붙잡고 네트워크를 기다리면 안 된다 | ⚠️ 5절 ★ |
| **`@EnableXxx` 누락** | 없으면 기능이 조용히 꺼진다 | ⚠️ 6절 |
| **의도적인 200 + 실패 상태** | 트랜잭션 커밋을 위한 우회. 구조를 고치면 불필요 | 7절 |
| **보상 실패 = 사람 개입** | 실패 작업 테이블 + 알림이 필요하다 | 8절 |
| **HTTP Basic 인증** | 시크릿 키 뒤 콜론이 필수 | 9절 |
| **파티션 키와 이벤트 순서** | `aggregateId`가 메시지 키가 된다 | 9절 |
| **커넥터 버전 관리** | 옛 커넥터가 남으면 이벤트가 중복 발행된다 | 9절 |

---

## 11. 개선 우선순위

| 순위 | 항목 | 심각도 | 근거 |
|---|---|---|---|
| 1 | 🚨 `StoreServiceClient` 경로를 `/api/v1/orders/...` 로 수정 | **매우 높음** | 404 → 결제 확인이 항상 실패 (2절) |
| 2 | 🚨 실패 outbox를 별도 트랜잭션으로 (또는 커밋 후 처리) | **매우 높음** | 보상 이벤트가 롤백되어 재고가 영구히 묶인다 (4절) |
| 3 | `@EnableJpaAuditing` 추가 | **높음** | 결제 기록에 시각이 없다 (6절) |
| 4 | `@Transactional` 밖으로 외부 HTTP 호출 분리 | **높음** | 토스 지연 시 커넥션 풀 고갈 (5절) |
| 5 | 보상 취소 실패를 실패작업 테이블 + 알림으로 | **높음** | 돈도 주문도 없는 상태가 로그에만 남는다 (8절) |
| 6 | 토스 에러를 `ErrorDecoder`로 구조화 | 중간 | 문자열 매칭이 깨지면 중복이 "실패"가 된다 (3절) |
| 7 | 취소 성공 시 `Payment.cancel()` 호출 | 중간 | 취소된 결제가 DB에 `DONE`으로 남는다 (9절) |
| 8 | 전역 예외 핸들러 + `ResponseDTO` 규약 적용 | 중간 | 결제 실패 응답 형식이 다른 서비스와 다르다 (9절) |
| 9 | `getApprovedAt()` null 방어 | 낮음 | 결제 재시도 경로에서 NPE (3절) |
| 10 | `DONE`이 아닌 상태도 `READY`로 저장 | 낮음 | 가상계좌 확장 시 필요 (7절) |
| 11 | `aggregateId`를 `orderId`로 통일 | 낮음 | `paymentKey`가 null일 수 있다 (9절) |
| 12 | Authorization 헤더를 `RequestInterceptor`로 | 낮음 | 중복 계산·중복 코드 (9절) |

**테스트를 하나만 쓴다면**: "DB 저장이 실패하면 `PAYMENT_FAILED` outbox 행이 남는다"를 권합니다.
2번 결함을 정확히 잡아내고, 이 서비스에서 돈과 재고가 어긋나는 유일한 경로를 지켜줍니다.

---

**다음 문서** → [08-infrastructure.md](08-infrastructure.md) — Kafka·Debezium·Elasticsearch·5노드 배포·모니터링
