# 06. store-service — 재고·주문·분산 락, 그리고 락이 지켜주지 못하는 지점

> 자바 파일 109개. 커머스라서 **돈과 재고**가 걸려 있고, 그래서 이 레포에서 **동시성 제어가 가장 많이
> 등장**하는 서비스입니다. Redisson 분산 락, Cache-Aside, Write-Behind 같은 고급 기법이 실제로 쓰여 있습니다.
>
> 그리고 **분산 락이 재고 초과 판매를 막지 못하는 구조적 오류**가 있습니다. 이 문서의 핵심입니다.

```
store-service/src/main/java/com/pawbridge/storeservice/
├── common/
│   ├── config/     RedissonConfig, CacheConfig, FeignConfig, JpaConfig
│   ├── entity/     Outbox
│   ├── repository/ OutboxRepository
│   └── service/    S3Service
└── domain/                            ★ 도메인별로 나눈 패키지 구조
    ├── product/    상품·SKU·재고   (controller/service/facade/entity/repository/dto)
    ├── option/     옵션 그룹·옵션 값
    ├── cart/       장바구니        (Redis 우선 + DB 동기화 스케줄러)
    ├── order/      주문·결제 수신  (consumer/ 포함)
    ├── wishlist/   찜한 상품
    ├── image/      이미지 업로드
    └── mypage/     마이페이지 전용 (user-service가 Feign으로 호출)
```

---

## 1. 패키지 구조가 다른 서비스와 다르다

user-service·animal-service는 **기술 계층별**로 나눴습니다.

```
userservice/
├── controller/     ← 모든 도메인의 컨트롤러가 여기
├── service/        ← 모든 도메인의 서비스가 여기
├── entity/
└── repository/
```

store-service는 **도메인별**로 나눴습니다.

```
storeservice/domain/
├── product/    controller + service + entity + repository + dto
├── cart/       controller + service + entity + repository + dto + scheduler
└── order/      controller + service + entity + repository + dto + consumer
```

**어느 쪽이 나은가** — 규모에 따라 다릅니다.

| | 계층별 (user-service) | 도메인별 (store-service) |
|---|---|---|
| 작은 서비스 | 파일을 찾기 쉽다 | 폴더가 과하게 깊다 |
| 큰 서비스 | `service/` 폴더에 20개 파일이 쌓인다 | **한 기능이 한 폴더에 모인다** |
| 변경 범위 | 기능 하나 고치려면 4개 폴더를 오간다 | **한 폴더 안에서 끝난다** |
| 나중에 분리 | 어렵다 | **폴더째로 떼어내면 된다** |

store-service는 도메인이 7개나 되므로 **도메인별이 적절한 선택**입니다.
실제로 "장바구니를 고친다"면 `domain/cart/` 안에서 컨트롤러·서비스·엔티티·스케줄러가 모두 보입니다.

⚠️ 다만 **한 레포 안에서 두 스타일이 섞여 있으면** 서비스를 옮겨 다닐 때 혼란스럽습니다.
지금은 서비스마다 독립적이라 큰 문제는 아니지만, 신규 서비스를 만들 때 어느 쪽을 따를지
정해두는 게 좋습니다.

---

## 2. 상품 모델 — Product / SKU / Option

커머스의 기본 모델입니다.

```
Product (상품)                     "강아지 사료"
  └─ OptionGroup (옵션 그룹)        "무게", "맛"
       └─ OptionValue (옵션 값)     "1kg", "3kg" / "닭고기", "소고기"
  └─ ProductSKU (재고 단위)         실제로 팔리고 재고를 세는 단위
       └─ SKUValue                  이 SKU가 어떤 옵션 조합인지 연결
```

**SKU(Stock Keeping Unit)** 는 "재고를 세는 최소 단위"입니다.

```
"강아지 사료"라는 Product 하나에 SKU 4개가 생긴다
  SKU-1: 1kg + 닭고기   재고 10, 가격 15,000
  SKU-2: 1kg + 소고기   재고  5, 가격 15,000
  SKU-3: 3kg + 닭고기   재고  3, 가격 38,000
  SKU-4: 3kg + 소고기   재고  0, 가격 38,000  ← 품절
```

**왜 Product에 재고를 두지 않나** — "사료 재고 18개"라는 정보는 쓸모가 없습니다.
사용자는 "3kg 소고기"를 사려는데, 그것만 품절일 수 있습니다.
**재고와 가격은 옵션 조합마다 다르므로 SKU 단위여야 합니다.**

```java
// ProductSKU 엔티티
@Column(nullable = false)
private Integer stockQuantity;      // ← 재고는 SKU에

private Long price;                  // ← 가격도 SKU에 (3kg가 더 비싸다)
private String skuCode;
```

`generateOptionName()`은 화면 표시용 문자열을 만듭니다.

```java
public String generateOptionName() {
    return this.skuValues.stream()
            .map(sv -> sv.getOptionValue().getOptionGroup().getName() + ": " + sv.getOptionValue().getName())
            .sorted()                             // ★ 정렬
            .reduce((a, b) -> a + ", " + b)
            .orElse("");
}
// → "맛: 닭고기, 무게: 3kg"
```

`.sorted()`를 넣은 것이 세심합니다. 옵션 순서가 DB 조회 순서에 따라 달라지면
**같은 SKU가 화면마다 "무게: 3kg, 맛: 닭고기"와 "맛: 닭고기, 무게: 3kg"로 다르게 보입니다.**
정렬하면 항상 같은 문자열이 나옵니다.

---

## 3. 주문 스냅샷 — 과거를 보존하는 설계

[domain/order/service/OrderServiceImpl.java](../store-service/src/main/java/com/pawbridge/storeservice/domain/order/service/OrderServiceImpl.java)

```java
OrderItem orderItem = OrderItem.builder()
        .order(order)
        .productSKU(sku)
        .productName(sku.getProduct().getName())   // Snapshot
        .skuCode(sku.getSkuCode())                  // Snapshot
        .price(sku.getPrice())                      // Snapshot
        .quantity(item.getQuantity())
        .build();
```

**SKU를 FK로 참조하면서 이름·코드·가격을 함께 복사해 둡니다.** 주석에 `// Snapshot`이라고 명시했습니다.

**왜 필요한가** — 주문 내역은 **"그때 무엇을 얼마에 샀는지"** 를 보여줘야 합니다.

```
1월 1일  "사료 3kg"을 38,000원에 주문
2월 1일  판매자가 가격을 45,000원으로 인상, 상품명을 "프리미엄 사료 3kg"으로 변경

스냅샷 없이 sku.getPrice()로 조회하면:
  → 주문 내역에 "프리미엄 사료 3kg  45,000원" 으로 표시  💀
  → 결제 금액(38,000)과 표시 금액(45,000)이 다르다 → 고객 분쟁

스냅샷이 있으면:
  → "사료 3kg  38,000원"  ✅ 영수증이 변하지 않는다
```

상품이 삭제되어도 주문 내역이 보존됩니다. **커머스에서 반드시 필요한 설계이고,
정확히 구현했습니다.**

> 💡 같은 이유로 배송지·수령인·연락처도 `Order`에 복사되어 있습니다.
> 사용자가 프로필의 주소를 바꿔도 과거 주문의 배송지는 그대로여야 합니다.

`orderUuid`를 별도로 두는 것도 의도적입니다.

```java
.orderUuid(UUID.randomUUID().toString())
```

DB의 `id`(auto increment)를 외부에 노출하면 **다른 사람의 주문 번호를 추측할 수 있습니다**
(`/api/orders/123` → `124`). UUID는 추측이 불가능합니다.
그리고 결제 서비스와 주고받는 식별자로 쓰기에도 안전합니다.

---

## 4. ★ Redisson 분산 락 — 그리고 여기서 발생한 치명적 오류

### 4-1. 왜 분산 락이 필요한가

재고 차감은 커머스에서 가장 유명한 동시성 문제입니다.

```java
// ProductSKU 엔티티
public void decreaseStock(int quantity) {
    if (this.stockQuantity < quantity) {
        throw new IllegalStateException("재고 부족");
    }
    this.stockQuantity -= quantity;      // ← 읽고-검사하고-쓰기
}
```

```
재고 1개, 두 사람이 동시에 주문

스레드 A                          스레드 B
stockQuantity 읽음 = 1
                                  stockQuantity 읽음 = 1
1 >= 1 → 통과
                                  1 >= 1 → 통과
0으로 UPDATE
                                  0으로 UPDATE
→ 재고 1개인데 2개가 팔렸다 (초과 판매, oversell) 💀
```

**자바의 `synchronized`로는 못 막습니다.** 서버가 2대면 각 JVM이 따로 잠그기 때문입니다.
그래서 **모든 서버가 공유하는 곳(Redis)에 락을 둡니다.**

### 4-2. Redisson 락의 사용법

[common/config/RedissonConfig.java](../store-service/src/main/java/com/pawbridge/storeservice/common/config/RedissonConfig.java)

```java
@Bean
public RedissonClient redissonClient() {
    Config config = new Config();
    config.setCodec(new JsonJacksonCodec());          // ★ 직렬화를 JSON으로
    config.useSingleServer().setAddress("redis://" + redisHost + ":" + redisPort);
    return Redisson.create(config);
}
```

`JsonJacksonCodec`을 지정한 것은 좋은 선택입니다. Redisson의 기본 코덱은 바이너리라
**Redis에 저장된 값을 사람이 읽을 수 없습니다.** JSON이면 `redis-cli`로 장바구니 내용을
바로 확인할 수 있어 디버깅이 쉽습니다.

```java
private static final String LOCK_KEY_PREFIX = "stock:sku:";
private static final long WAIT_TIME = 5L;      // 락을 얻으려고 기다릴 시간
private static final long LEASE_TIME = 3L;     // 락을 자동으로 놓는 시간

RLock lock = redissonClient.getLock(lockKey);
try {
    boolean available = lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
    if (!available) {
        throw new RuntimeException("System is busy. Please try again later.");
    }
    ... 임계 구역 ...
} finally {
    if (lock.isHeldByCurrentThread()) {
        lock.unlock();
    }
}
```

**두 시간 값의 의미**

- **`WAIT_TIME` (대기)** — 다른 스레드가 락을 쥐고 있을 때 몇 초까지 기다릴지.
  넘으면 `false`를 반환합니다. `lock()`(무한 대기) 대신 `tryLock()`을 쓴 것이 정확합니다.
  무한 대기는 트래픽이 몰리면 요청 스레드가 전부 대기 상태에 빠져 서비스가 멈춥니다.
- **`LEASE_TIME` (임대)** — 이 시간이 지나면 **락이 자동으로 풀립니다.**
  이게 없으면 락을 쥔 서버가 갑자기 죽었을 때 **아무도 그 락을 풀 수 없어 영구 교착**이 됩니다.
  분산 락에서 반드시 필요한 안전장치입니다.

**`isHeldByCurrentThread()` 확인도 필수입니다.**
`LEASE_TIME`이 지나 락이 자동 해제된 뒤 `unlock()`을 호출하면 예외가 납니다.
더 나쁜 경우, 그 사이 다른 스레드가 얻은 락을 **내가 풀어버릴 수도** 있습니다.

### 4-3. 데드락 방지 — 자원 순서화

```java
// 2. Sort Items by SKU ID to prevent Deadlock (Resource Ordering)
cartItems.sort(Comparator.comparing(CartItemResponse::getSkuId));
```

여러 개의 락을 잡을 때 **순서가 엇갈리면 교착이 생깁니다.**

```
A: SKU-1 잠금 → SKU-2 잠그려 대기
B: SKU-2 잠금 → SKU-1 잠그려 대기
→ 둘 다 영원히 기다린다 (데드락)
```

**모두가 같은 순서(ID 오름차순)로 잠그면 이 상황이 생길 수 없습니다.**
운영체제 교과서에 나오는 정석 해법이고, 이걸 알고 적용한 것은 인상적입니다.

> 참고: 지금 코드는 반복문 안에서 락을 **얻고 바로 놓기** 때문에 동시에 두 개 이상을 쥐지 않아
> 데드락이 애초에 불가능합니다. 즉 이 정렬은 현재 구현에서는 필요하지 않습니다.
> 다만 아래 4-4의 문제를 고쳐 **락을 트랜잭션 끝까지 유지**하게 되면, 그때는
> **이 정렬이 반드시 필요해집니다.** 미리 넣어둔 셈이 됩니다.

### 4-4. 🚨 락이 트랜잭션보다 먼저 풀린다 — 초과 판매를 막지 못한다

이것이 이 서비스의 가장 중요한 문제입니다.

```java
@Transactional                                    // ← 트랜잭션 시작
public OrderResponse createOrder(Long userId, OrderCreateRequest request) {
    for (CartItemResponse item : cartItems) {
        RLock lock = redissonClient.getLock(lockKey);
        try {
            lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
            productService.decreaseStock(item.getSkuId(), item.getQuantity());   // JPA 변경
        } finally {
            lock.unlock();                        // ← 🚨 락 해제
        }
    }
    orderRepository.save(order);
    cartService.clearCart(userId);
}                                                  // ← 여기서 트랜잭션 COMMIT
```

**JPA는 `decreaseStock()`을 호출한 순간 `UPDATE`를 보내지 않습니다.**
엔티티의 필드만 바꿔두고, 실제 SQL은 **트랜잭션이 커밋될 때(flush)** 나갑니다.

그래서 시간 순서가 이렇게 됩니다.

```
스레드 A                                   스레드 B
─────────────────────────────────────────────────────────────
락 획득 (stock:sku:1)
재고 읽음 = 1                               ← 락 대기
1 >= 1 통과, 메모리에서 0으로 변경
락 해제  ★ 아직 커밋 안 됨!
                                           락 획득 (성공!)
                                           재고 읽음 = 1  ★ A가 커밋 안 했으니 옛 값
                                           1 >= 1 통과
                                           락 해제
COMMIT → UPDATE stock = 0
                                           COMMIT → UPDATE stock = 0
─────────────────────────────────────────────────────────────
결과: 재고 1개인데 주문 2건. 락을 걸었는데도 초과 판매가 발생한다.
```

**즉 이 락은 아무것도 보호하지 않습니다.** 임계 구역이 "재고를 읽고 메모리에서 계산하는 구간"만
덮고 있고, **정작 중요한 "DB에 확정하는 구간"이 밖에 있습니다.**

> 📌 **분산 락의 철칙: 락은 트랜잭션보다 넓어야 한다.**
> ```
> ❌ 트랜잭션 { 락 { 작업 } }        ← 지금 코드
> ✅ 락 { 트랜잭션 { 작업 } }        ← 올바른 순서
> ```

**추가 문제: `LEASE_TIME`(3초)이 `WAIT_TIME`(5초)보다 짧습니다.**

락을 트랜잭션 밖으로 옮겨 고친다 해도, 주문 처리가 3초를 넘으면 **작업 중에 락이 자동 해제**됩니다.
장바구니 상품이 많거나 DB가 느릴 때 충분히 가능합니다.
`LEASE_TIME`은 "최악의 처리 시간"보다 넉넉해야 하고, `WAIT_TIME`보다 커야 합니다.

#### 고치는 방법 — 세 가지 선택지

**(A) 원자적 UPDATE — 가장 간단하고, 이 경우 가장 좋다**

```java
// ProductSKURepository
@Modifying
@Query("UPDATE ProductSKU s SET s.stockQuantity = s.stockQuantity - :qty " +
       "WHERE s.id = :id AND s.stockQuantity >= :qty")
int decreaseStock(@Param("id") Long id, @Param("qty") int qty);

// 서비스
int updated = productSKURepository.decreaseStock(skuId, quantity);
if (updated == 0) {
    throw new OutOfStockException("재고가 부족합니다. skuId=" + skuId);
}
```

**락이 아예 필요 없어집니다.** DB가 `UPDATE` 한 문장을 원자적으로 처리하고,
`WHERE stockQuantity >= :qty` 조건이 재고 부족을 함께 판정합니다.
영향받은 행 수가 0이면 재고가 모자랐다는 뜻입니다.

- Redis 왕복이 사라져 **더 빠릅니다.**
- Redis가 죽어도 주문이 됩니다(현재는 Redis 장애 = 주문 불가).
- 데드락도 없습니다(단일 문장).

**(B) 비관적 락 (`SELECT ... FOR UPDATE`)**

```java
@Lock(LockModeType.PESSIMISTIC_WRITE)
@Query("SELECT s FROM ProductSKU s WHERE s.id = :id")
Optional<ProductSKU> findByIdForUpdate(@Param("id") Long id);
```

DB가 트랜잭션이 끝날 때까지 그 행을 잠급니다. **락과 트랜잭션의 범위가 자동으로 일치**하므로
지금과 같은 오류가 구조적으로 발생하지 않습니다. 재고 검사 로직을 자바에 남기고 싶을 때 좋습니다.

**(C) 분산 락을 유지하되 순서를 고친다**

```java
// 트랜잭션을 락 안쪽으로 넣는다
public OrderResponse createOrder(Long userId, OrderCreateRequest request) {   // @Transactional 제거
    List<RLock> locks = acquireLocksInOrder(skuIds);     // 정렬해서 전부 획득
    try {
        return orderTransactionalService.createOrderInTx(userId, request);   // 여기에 @Transactional
    } finally {
        releaseAll(locks);
    }
}
```

Redisson의 `RedissonMultiLock`을 쓰면 여러 락을 한 번에 얻을 수 있습니다.
다만 **주문 전체가 끝날 때까지 모든 SKU가 잠기므로 처리량이 떨어집니다.**

**결론: (A)를 권합니다.** 재고 차감은 "덧셈/뺄셈 + 하한 검사"라서 DB가 가장 잘하는 일입니다.
분산 락은 "여러 자원에 걸친 복잡한 불변식"을 지킬 때 쓰는 것이 맞습니다.

### 4-5. `createDirectOrder`의 검증 순서

```java
// 1. Lock & Stock Deduction
lock.tryLock(...);
productService.decreaseStock(skuId, quantity);      // ① 재고 차감
lock.unlock();

// 2. Fetch Product Info for Snapshot
ProductSKU sku = productSKURepository.findById(skuId)...;
if (productStatus != ProductStatus.ACTIVE) {         // ② 상품 상태 검증
    throw new IllegalStateException("주문할 수 없는 상품입니다...");
}
```

**차감을 먼저 하고 판매 가능 여부를 나중에 검사합니다.** 예외가 나면 트랜잭션이 롤백되어
재고는 복구되므로 데이터가 틀어지지는 않습니다. 다만:

- 삭제된 상품에 대한 요청이 **불필요하게 락을 잡고 DB를 씁니다.**
- `createOrder`(장바구니 주문)는 **검증을 먼저** 합니다. 두 메서드의 순서가 다릅니다.

```java
// createOrder — 이쪽이 맞다
ProductSKU sku = productSKURepository.findById(item.getSkuId())...;
if (sku.getProduct().getStatus() != ProductStatus.ACTIVE) throw ...;   // 먼저 검증
productService.decreaseStock(item.getSkuId(), item.getQuantity());     // 그다음 차감
```

`createDirectOrder`도 같은 순서로 맞추는 게 좋습니다.

### 4-6. 재고 복구 — `cancelOrder`

```java
public void cancelOrder(String orderUuid) {
    Order order = orderRepository.findByOrderUuid(orderUuid)...;
    if (order.getStatus() == OrderStatus.CANCELLED) {
        log.warn("Order {} is already canceled.", orderUuid);
        return;                                   // ★ 멱등성
    }
    order.cancelOrder();
    for (OrderItem item : order.getOrderItems()) {
        ... 락 획득 → productService.increaseStock(...) → 락 해제 ...
    }
}
```

**이미 취소된 주문이면 그냥 반환하는 것이 중요합니다.**
결제 실패 이벤트가 중복 수신되어도 재고가 두 번 복구되지 않습니다. 멱등성을 확보했습니다.

⚠️ 다만 `createOrder`와 **같은 락 오류**를 갖고 있습니다(락이 커밋보다 먼저 풀림).
그리고 `increaseStock`은 하한 검사가 없어 **중복 실행 시 재고가 부풀어 오릅니다.**
`order.getStatus()` 검사가 그 방어인데, 그 검사와 재고 복구가 커밋 전에 이뤄지므로
동시에 두 번 호출되면 둘 다 통과할 수 있습니다.

**여기도 (A) 방식이 답입니다.** 상태 변경을 조건부 UPDATE로 만들면 중복이 원천 차단됩니다.

```java
@Modifying
@Query("UPDATE Order o SET o.status = 'CANCELLED' WHERE o.orderUuid = :uuid AND o.status <> 'CANCELLED'")
int cancelIfNotCancelled(@Param("uuid") String uuid);
// 반환값이 1이면 "내가 처음 취소했다" → 재고 복구를 진행
// 반환값이 0이면 이미 취소됨 → 아무것도 하지 않는다
```

---

## 5. ★ Cache-Aside + 캐시 스탬피드 방지

[domain/product/facade/ProductFacade.java](../store-service/src/main/java/com/pawbridge/storeservice/domain/product/facade/ProductFacade.java)

상품 상세 조회는 트래픽이 가장 많은 API입니다. 그래서 캐시를 붙였는데,
**단순한 캐시가 아니라 세 가지 기법이 겹쳐 있습니다.**

```java
public ProductDetailResponse getProductDetails(Long productId) {
    // 1. [Cache-Aside] 캐시 확인
    ProductDetailResponse cached = getFromCache(cacheKey);
    if (cached != null) return cached;                       // HIT

    RLock lock = redissonClient.getLock(lockKey);
    try {
        // 2. [Distributed Lock] 락 획득
        boolean available = lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
        if (!available) {
            return productService.getProductDetails(productId);   // 락 실패 → DB 직접 조회
        }

        // 3. [Double-Check] 캐시 재확인
        cached = getFromCache(cacheKey);
        if (cached != null) return cached;

        // 4. [DB Load]
        ProductDetailResponse response = productService.getProductDetails(productId);

        // 5. [Cache Write] Jitter TTL
        long baseTtl = 600;                                        // 10분
        long jitter = ThreadLocalRandom.current().nextLong(0, 60);  // 0~60초 랜덤
        redisTemplate.opsForValue().set(cacheKey, response, Duration.ofSeconds(baseTtl + jitter));

        return response;
    } finally {
        if (lock.isHeldByCurrentThread()) lock.unlock();
    }
}
```

### 5-1. Cache-Aside 패턴

```
읽기: 캐시 확인 → 없으면 DB 조회 → 캐시에 저장 → 반환
쓰기: DB 수정 → 캐시 삭제(무효화)
```

`ProductServiceImpl.decreaseStock()`에 `cacheService.evictProductCache(productId)`가 있는 것이
쓰기 쪽 무효화입니다. **재고가 줄었으니 캐시된 상품 정보를 지웁니다.**

"수정" 대신 "삭제"를 하는 이유는, 수정하려면 캐시에 뭘 넣을지 계산해야 하고
그 과정에서 동시성 문제가 또 생기기 때문입니다. **삭제하고 다음 요청이 다시 채우게 하는 것**이
단순하고 안전합니다.

### 5-2. 락 + Double-Check — 캐시 스탬피드(Thundering Herd) 방지

**문제 상황**: 인기 상품의 캐시가 만료되는 순간

```
락이 없으면:
  캐시 만료 → 동시 요청 1,000개가 전부 MISS
  → 1,000개가 동시에 같은 DB 쿼리를 던진다
  → DB 커넥션 풀 고갈 → 전체 서비스 장애 💀
```

락을 걸면 **한 스레드만 DB를 조회**하고 나머지는 기다립니다.
기다렸다 락을 얻은 스레드는 **다시 캐시를 확인**합니다(Double-Check).
이미 앞선 스레드가 채워놨으므로 DB에 가지 않습니다.

```
락 + Double-Check:
  1,000개 요청 → 1개만 DB 조회 → 나머지 999개는 캐시에서 HIT
  → DB 쿼리 1번 ✅
```

**락 획득 실패 시 DB를 직접 조회하는 것도 좋은 판단입니다.**

```java
if (!available) {
    log.warn(">>> [LOCK FAIL] Could not acquire lock for productId: {}", productId);
    return productService.getProductDetails(productId);      // 캐시 없이 응답
}
```

"락을 못 얻었으니 실패"보다 "조금 느리지만 응답은 한다"가 사용자에게 낫습니다.
**가용성을 위해 최적화를 포기하는 선택**입니다.

### 5-3. Jitter TTL — 만료 시각을 흩뜨린다

```java
long baseTtl = 600;
long jitter = ThreadLocalRandom.current().nextLong(0, 60);
redisTemplate.opsForValue().set(cacheKey, response, Duration.ofSeconds(baseTtl + jitter));
```

**문제**: TTL이 모두 정확히 600초면, 서버 재시작 직후 캐시된 상품 1,000개가
**정확히 같은 순간에 모두 만료**됩니다. 상품별로는 락이 걸려 있지만, 1,000개 상품이
동시에 각자 DB를 치면 결국 DB가 무너집니다.

**해법**: TTL에 0~60초의 무작위 값을 더해 **만료 시각을 60초 구간에 흩뿌립니다.**

```
Jitter 없음:                        Jitter 있음:
600초 시점 ████████████ 1000개      600~660초 ▁▂▃▄▅▄▃▂▁ 균등 분산
           ↑ 순간 부하 폭발                    ↑ 부하가 평탄해진다
```

**이건 대규모 캐시 운영에서 나오는 기법입니다.** 학습 프로젝트에서 이걸 적용한 건 드뭅니다.

### 5-4. ⚠️ 이 코드의 문제점

```java
// 4. [DB Load] Call Service
log.info(">>> [CACHE MISS] Loading from DB... ProductId: {}", productId);
try {
    // Thread.sleep(3000); // [테스트] 제거됨
} catch (Exception e) {}                      // ← 🚨 빈 try-catch가 남았다
```

테스트용 `Thread.sleep`을 주석 처리하면서 **빈 `try-catch`만 남았습니다.**
지금은 무해하지만, 이런 빈 catch는 나중에 누군가 그 안에 코드를 넣었을 때
**예외가 조용히 사라지는 함정**이 됩니다. 삭제해야 합니다.

```java
// LEASE_TIME = 3초인데, DB 조회가 3초를 넘으면?
boolean available = lock.tryLock(WAIT_TIME, LEASE_TIME, TimeUnit.SECONDS);
```

**락이 자동 해제되어 다른 스레드가 동시에 DB를 조회합니다.**
상품 상세 조회에 옵션·SKU 조인이 많으면 3초를 넘을 수 있습니다.
스탬피드 방지가 가장 필요한 순간(DB가 느릴 때)에 방지가 풀리는 셈입니다.

```java
// Redisson의 watchdog 기능을 쓰면 자동 갱신된다
lock.tryLock(WAIT_TIME, TimeUnit.SECONDS);   // leaseTime을 주지 않으면 watchdog이 30초마다 연장
```

```java
private ProductDetailResponse getFromCache(String key) {
    try {
        Object data = redisTemplate.opsForValue().get(key);
        if (data instanceof ProductDetailResponse) return (ProductDetailResponse) data;
        else if (data != null) return objectMapper.convertValue(data, ProductDetailResponse.class);
    } catch (Exception e) {
        log.warn("Cache parsing error", e);       // 파싱 실패 → 캐시 미스로 처리
    }
    return null;
}
```

주석에 *"여기서는 적절히 변환된다고 가정"* 이라고 적혀 있습니다.
**파싱 실패를 캐시 미스로 처리하는 것 자체는 옳습니다** — 캐시가 깨졌으면 DB에서 다시 읽으면 됩니다.
다만 DTO 필드를 추가/변경하면 **기존 캐시가 전부 파싱 실패**하므로,
배포 후 한동안 캐시가 무력해집니다. 캐시 키에 버전을 넣으면(`productDetails:v2::123`)
배포 시 자연스럽게 새 캐시로 넘어갑니다.

---

## 6. ★ Write-Behind 장바구니 — Redis를 1차 저장소로

[domain/cart/service/CartServiceImpl.java](../store-service/src/main/java/com/pawbridge/storeservice/domain/cart/service/CartServiceImpl.java)

### 6-1. 왜 장바구니를 Redis에 두나

장바구니의 성질을 보면 답이 나옵니다.

| 성질 | 결론 |
|---|---|
| 담기/빼기가 매우 빈번 | DB 쓰기가 계속 발생 → 부하 |
| 잠깐 사라져도 치명적이지 않음 | 주문 전 임시 데이터 |
| 최종 확정은 주문 시점 | 그때 DB와 재고를 정확히 확인하면 됨 |

```java
private RMap<Long, Integer> getCartMap(Long userId) {
    return redissonClient.getMap(CART_KEY_PREFIX + userId);      // cart:{userId}
}

public void addToCart(Long userId, CartAddRequest request) {
    ProductSKU sku = productSKURepository.findById(request.getSkuId())...;   // 재고 검증
    if (sku.getStockQuantity() < request.getQuantity()) throw ...;

    RMap<Long, Integer> cartMap = getCartMap(userId);
    cartMap.addAndGet(request.getSkuId(), request.getQuantity());   // ★ Redis에만 쓴다
    markAsDirty(userId);
}
```

**`RMap`은 Redisson이 제공하는 분산 Map**입니다. Redis의 Hash 타입을 자바 `Map`처럼 씁니다.
`addAndGet()`은 **원자적 증감**이므로, 같은 사용자가 두 탭에서 동시에 담아도 수량이 정확합니다.
(재고 차감에는 이런 원자적 연산을 안 쓴 것이 아쉬운 부분입니다 — [4-4](#4-4--락이-트랜잭션보다-먼저-풀린다--초과-판매를-막지-못한다) 참고.)

### 6-2. Dirty Set — 바뀐 사용자만 기록한다

```java
private static final String DIRTY_USERS_KEY = "cart:dirty-users";

private void markAsDirty(Long userId) {
    RSet<Long> dirtySet = redissonClient.getSet(DIRTY_USERS_KEY);
    dirtySet.add(userId);
}
```

장바구니를 바꾼 사용자의 ID를 **Redis Set에 모아둡니다.**

`Set`을 쓴 것이 핵심입니다. 한 사용자가 10번 담아도 **집합에는 한 번만 들어갑니다.**
동기화할 때 그 사용자를 한 번만 처리하면 됩니다. (`List`면 10번 처리하게 됩니다.)

### 6-3. Write-Behind 스케줄러

[domain/cart/scheduler/CartSyncScheduler.java](../store-service/src/main/java/com/pawbridge/storeservice/domain/cart/scheduler/CartSyncScheduler.java)

```java
@Scheduled(fixedDelay = 10000)      // 10초마다
@Transactional
public void syncCartsToDb() {
    RSet<Long> dirtySet = redissonClient.getSet(DIRTY_USERS_KEY);
    if (dirtySet.isEmpty()) return;

    Iterator<Long> iterator = dirtySet.iterator();
    while (iterator.hasNext()) {
        Long userId = iterator.next();
        try {
            processUserCart(userId);
            dirtySet.remove(userId);       // 성공 후에만 제거
        } catch (Exception e) {
            log.error("Failed to sync cart for userId: {}", userId, e);
            // 제거하지 않음 → 다음 실행에서 재시도
        }
    }
}
```

**Write-Behind(= Write-Back) 패턴**입니다.

```
Write-Through: 요청 → 캐시 쓰기 + DB 쓰기 (동시)     → 느리지만 항상 일치
Write-Behind:  요청 → 캐시 쓰기만 → 나중에 DB 반영   → 빠르지만 잠시 불일치  ★
```

**`fixedDelay`와 `fixedRate`의 차이도 짚어둘 만합니다.**

```
fixedRate  = 10초  →  시작 시점 기준 10초마다. 작업이 15초 걸리면 다음 실행이 겹친다.
fixedDelay = 10초  →  끝난 뒤 10초 후. 절대 겹치지 않는다.  ★
```

동기화 작업은 겹치면 안 되므로 `fixedDelay`가 맞습니다.

**실패 시 dirty 플래그를 남겨 재시도하게 한 것도 좋습니다.**

### 6-4. ⚠️ 그런데 여기 심각한 문제가 있습니다

```java
@Transactional                    // ← 🚨 전체 사용자가 하나의 트랜잭션
public void syncCartsToDb() {
    while (iterator.hasNext()) {
        ...
        processUserCart(userId);
        dirtySet.remove(userId);   // ← 🚨 Redis 작업은 트랜잭션과 무관하게 즉시 반영
        ...
    }
}
```

**문제 ①: 모든 사용자가 하나의 트랜잭션 안에 있습니다.**

```
사용자 1~4 처리 성공 → Redis dirty 플래그 제거됨 (즉시 반영, 롤백 안 됨)
사용자 5 처리 중 DB 예외 발생
  → catch가 로그를 남기고 넘어감
  → 그런데 JPA는 이미 트랜잭션을 "롤백 전용"으로 표시함
  → 메서드가 끝날 때 커밋 시도 → UnexpectedRollbackException
  → 사용자 1~4의 DB 변경도 전부 롤백  💀
  → 하지만 그들의 dirty 플래그는 이미 지워졌다 → 영구히 동기화되지 않는다
```

**장바구니 데이터가 조용히 유실됩니다.**

**문제 ②: Redis와 DB의 커밋 시점이 다릅니다.**
`dirtySet.remove()`는 즉시 반영되고 DB는 나중에 커밋됩니다.
"성공 후에만 제거"라는 의도가 지켜지지 않습니다 — 실제로는 **"커밋 전에 제거"** 입니다.

**문제 ③: 인스턴스가 여러 대면 같은 dirty set을 동시에 처리합니다.**
중복 작업이고, 서로의 `remove()`가 엇갈립니다. Redisson 락이 이미 있으니 활용하면 됩니다.

#### 고치는 방법

```java
@Scheduled(fixedDelay = 10000)
public void syncCartsToDb() {                          // ★ @Transactional 제거
    RLock lock = redissonClient.getLock("lock:cart-sync");
    if (!lock.tryLock(0, 30, TimeUnit.SECONDS)) return;  // ★ 인스턴스 하나만 실행
    try {
        RSet<Long> dirtySet = redissonClient.getSet(DIRTY_USERS_KEY);
        for (Long userId : new ArrayList<>(dirtySet)) {   // 스냅샷을 떠서 순회
            try {
                cartSyncService.syncOne(userId);          // ★ 사용자별 트랜잭션 (별도 빈)
                dirtySet.remove(userId);                  // ★ 커밋 성공 후 제거
            } catch (Exception e) {
                log.error("Failed to sync cart for userId: {}", userId, e);
                // 플래그 유지 → 다음 회차 재시도
            }
        }
    } finally {
        if (lock.isHeldByCurrentThread()) lock.unlock();
    }
}
```

**`@Transactional`을 별도 빈의 메서드에 두는 것이 핵심입니다.**
같은 클래스 안에서 `this.processUserCart()`를 호출하면 **프록시를 거치지 않아 `@Transactional`이
적용되지 않습니다.** 사용자별로 트랜잭션을 분리하려면 반드시 다른 빈이어야 합니다.

> 💡 **원칙 재확인**: [05-community-service.md](05-community-service.md#6-1--이미지-수정-순서--삭제가-업로드보다-먼저다)에서
> 본 것과 같은 문제입니다. **트랜잭션 밖의 시스템(Redis, S3, 메일)에 대한 되돌릴 수 없는 작업은
> 커밋 이후에 해야 합니다.** 이 레포에서 세 번 반복되는 실수입니다(S3 삭제, Redis dirty 제거, 분산 락 해제).

### 6-5. 동기화 로직 — Overwrite 전략

```java
private void updateDbCartItems(Cart dbCart, Map<Long, Integer> redisItems) {
    // 1. 있으면 수정, 없으면 추가
    redisItems.forEach((skuId, qty) -> {
        Optional<CartItem> existing = dbCart.getItems().stream()
                .filter(item -> item.getSkuId().equals(skuId))
                .findFirst();
        if (existing.isPresent()) existing.get().updateQuantity(qty);
        else dbCart.addItem(new CartItem(skuId, qty));
    });
    // 2. Redis에 없는 항목은 DB에서 제거
    dbCart.getItems().removeIf(item -> !redisItems.containsKey(item.getSkuId()));
}
```

**Redis를 진실의 원천으로 삼아 DB를 그대로 맞춥니다.** 2번(제거)이 있어야
사용자가 뺀 상품이 DB에 남지 않습니다. 방향이 명확합니다.

⚠️ 이중 루프라 항목 수의 곱만큼 비교합니다(`O(n×m)`). 장바구니 항목이 적으니 실무상 문제는 없지만,
Map으로 한 번 바꾸면 선형이 됩니다.

```java
Map<Long, CartItem> byId = dbCart.getItems().stream()
        .collect(toMap(CartItem::getSkuId, identity()));
```

### 6-6. `getMyCart`의 조용한 데이터 손실

```java
Map<Long, Integer> itemMap = cartMap.readAllMap();
List<ProductSKU> skus = productSKURepository.findAllById(itemMap.keySet());
return skus.stream()
        .map(sku -> CartItemResponse.of(sku, itemMap.get(sku.getId())))
        .toList();
```

**SKU 목록을 기준으로 순회**하므로, DB에서 삭제된 SKU는 응답에서 빠집니다.
사용자에게 안 보이니 괜찮아 보이지만, **Redis에는 그 항목이 계속 남아 있습니다.**
장바구니를 열 때마다 `findAllById`에 없는 ID를 계속 조회합니다.

```java
// 개선: 조회 시 사라진 SKU를 Redis에서도 정리한다
Set<Long> foundIds = skus.stream().map(ProductSKU::getId).collect(toSet());
itemMap.keySet().stream().filter(id -> !foundIds.contains(id))
        .forEach(cartMap::remove);
```

---

## 7. Outbox와 결제 이벤트 수신

### 7-1. Outbox 엔티티에 `eventId`가 없다

[common/entity/Outbox.java](../store-service/src/main/java/com/pawbridge/storeservice/common/entity/Outbox.java)

```java
@Table(name = "outbox")
public class Outbox {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;                      // ← 이게 event id 역할
    private String aggregateType;          // "ORDER"
    private String aggregateId;
    private String eventType;              // "ORDER_PAID"
    private String payload;
    private LocalDateTime createdAt;
}
```

다른 세 서비스는 `eventId`(UUID) 컬럼을 따로 두었는데, 여기는 **PK를 그대로 씁니다.**
커넥터 설정도 그에 맞춰져 있습니다.

```json
"transforms.outbox.table.field.event.id": "id"      // ← 다른 커넥터는 "event_id"
```

**기능적으로는 동작합니다.** auto increment PK도 고유하므로 중복 판별에 쓸 수 있습니다.

다만 차이가 있습니다.

| | UUID `eventId` | auto increment `id` |
|---|---|---|
| 트랜잭션 커밋 전에 값을 알 수 있나 | ⭕ (미리 생성) | ❌ (DB가 부여) |
| 발행자가 로그에 남길 수 있나 | ⭕ | 커밋 후에만 |
| DB를 옮기거나 복원했을 때 | 항상 유일 | 번호가 겹칠 수 있다 |

`store-outbox-connector`의 이름이 `store-outbox-connector-v3`이고
`topic.prefix`가 `store-v3`, 스키마 히스토리 토픽은 `schema-changes.store-v2`인 점도 눈에 띕니다.
**설정을 여러 번 고쳐가며 버전을 올린 흔적**이고, v3와 v2가 섞여 있습니다.
Debezium 커넥터는 `topic.prefix`를 바꾸면 오프셋이 초기화되므로 재설정이 잦습니다.
정리해 두는 게 좋습니다.

### 7-2. `store.outbox.events`를 아무도 소비하지 않는다

```java
// processPayment()
Outbox outbox = Outbox.builder()
        .aggregateType("ORDER")
        .aggregateId(String.valueOf(order.getId()))
        .eventType("ORDER_PAID")
        .payload(payload)
        .build();
outboxRepository.save(outbox);
```

이 이벤트는 `store.outbox.events` 토픽으로 갑니다.
**그런데 이 레포에 그 토픽을 구독하는 코드가 없습니다.**

이벤트를 발행하는 것 자체는 나쁘지 않습니다("나중에 알림 서비스가 붙을 것"),
다만 **지금은 Kafka에 데이터만 쌓이고 아무 일도 하지 않습니다.**
의도한 것이라면 주석으로 남겨두는 게 좋습니다. 아니라면 소비자가 누락된 것입니다.

### 7-3. 🚨 결제 이벤트가 조용히 유실된다

[domain/order/consumer/PaymentEventConsumer.java](../store-service/src/main/java/com/pawbridge/storeservice/domain/order/consumer/PaymentEventConsumer.java)

```java
@Transactional
@KafkaListener(topics = {"payment.events", "payment"}, groupId = "payment-group",
               containerFactory = "kafkaListenerContainerFactory")
public void handlePaymentEvents(String message) {
    try {
        JsonNode root = objectMapper.readTree(message);
        ...
        if ("DONE".equals(status))                                    completeOrder(orderId);
        else if ("ABORTED".equals(status) || "CANCELED".equals(status)) orderService.cancelOrder(orderId);
    } catch (JsonProcessingException e) {
        log.error("Payment Event Parsing Failed", e);
    } catch (Exception e) {
        log.error("Payment Event Processing Failed", e);      // 🚨 삼키고 끝
    }
}
```

**예외를 삼키므로 Kafka 오프셋이 정상 커밋됩니다.** 그 메시지는 다시 오지 않습니다.

```
사용자가 결제 완료 → 토스가 승인 → payment-service가 이벤트 발행
→ store-service가 수신 → DB 일시 장애로 예외
→ 로그 한 줄만 남고 오프셋 커밋
→ 주문은 영원히 PENDING. 재고는 이미 차감됨. 돈은 이미 받았음.  💀
```

**이 레포에서 가장 위험한 코드입니다.** 돈이 걸려 있고, 조용히 실패하며,
사용자가 문의하기 전까지 아무도 모릅니다.

**바로 옆 animal-service가 이미 정답을 갖고 있습니다.**

```java
} catch (Exception e) {
    log.error("Payment Event Processing Failed", e);
    throw new RuntimeException(e);        // ★ ack 하지 않음 → 재시도
}
```

그리고 [animal-service의 `DefaultErrorHandler` 설정](04-animal-service.md#5-5--defaulterrorhandler--재시도-3회-그다음-보상)을
복사해 붙이면 재시도 + DLT까지 갖춰집니다.

```java
@Bean
public CommonErrorHandler paymentErrorHandler(KafkaTemplate<String, Object> template) {
    // 3회 재시도 후 payment.events.DLT 로 이동
    return new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(template),
            new FixedBackOff(1000L, 3L));
}
```

DLT(Dead Letter Topic)에 남으면 나중에 사람이 확인하고 재처리할 수 있습니다.
**돈이 걸린 이벤트는 절대 사라지면 안 됩니다.**

### 7-4. 토픽을 두 개 구독하는 이유

```java
@KafkaListener(topics = {"payment.events", "payment"}, ...)
```

**같은 이벤트를 위해 토픽 두 개를 구독합니다.** 이건 보통 "커넥터가 어느 토픽으로 보내는지
확신이 없어서 둘 다 걸어둔" 흔적입니다.

위험한 점: 만약 두 토픽에 **같은 이벤트가 모두 들어오면 두 번 처리**됩니다.
`completeOrder`가 `status == PAID`를 확인하므로 결제 완료는 멱등하지만,
`cancelOrder`도 상태 검사가 있어 대체로 안전합니다. 다만 **의도한 안전장치가 아니라 우연**입니다.

[07-payment-service.md](07-payment-service.md)에서 실제 커넥터가 어느 토픽으로 보내는지 확인하고,
**하나로 줄이는 것이 맞습니다.**

### 7-5. 멱등성이 상태 검사에만 의존한다

```java
private void completeOrder(String orderUuid) {
    Order order = orderRepository.findByOrderUuid(orderUuid)...;
    if (order.getStatus() == OrderStatus.PAID) {
        log.info("Order {} is already paid.", orderUuid);
        return;                                 // 중복 방어
    }
    order.paid();
    updateRanking(order);
}
```

다른 세 서비스는 `processed_events` 테이블로 `eventId`를 기록해 중복을 막습니다.
여기는 **주문 상태로 판단**합니다.

**대체로 동작하지만 두 가지 빈틈이 있습니다.**

1. **동시 수신 시 둘 다 통과할 수 있습니다.** 상태 검사와 `order.paid()` 사이에 커밋이 없으므로,
   두 스레드가 같은 이벤트를 동시에 처리하면 둘 다 `PENDING`을 읽습니다.
   그러면 `updateRanking()`이 **두 번 실행되어 랭킹 점수가 두 배**가 됩니다.
2. **다른 종류의 이벤트에는 적용할 수 없습니다.** 상태를 바꾸지 않는 이벤트(예: 알림 발송)는
   이 방식으로 중복을 막을 수 없습니다.

`processed_events` 방식으로 통일하거나, 최소한 조건부 UPDATE로 바꾸는 게 안전합니다.

```java
@Modifying
@Query("UPDATE Order o SET o.status = 'PAID' WHERE o.orderUuid = :uuid AND o.status = 'PENDING'")
int markPaidIfPending(@Param("uuid") String uuid);
// 1을 반환한 스레드만 랭킹을 갱신한다
```

### 7-6. Debezium 메시지 구조를 방어적으로 파싱한다

```java
// 1. Debezium 'payload' Field Extraction (if wrapped)
if (root.has("payload") && root.get("payload").isTextual()) {
    root = objectMapper.readTree(root.get("payload").asText());     // 문자열이면 다시 파싱
} else if (root.has("payload") && root.get("payload").isObject()) {
    root = root.get("payload");                                     // 객체면 꺼내기
}
```

**세 가지 경우를 모두 처리합니다**: `payload`가 없는 경우, 문자열인 경우, 객체인 경우.

`payload`가 문자열로 오는 건 커넥터에 `table.expand.json.payload: true`가 없을 때이고,
객체로 오는 건 있을 때입니다. `schemas.enable` 설정에 따라 한 겹이 더 생깁니다.
**커넥터 설정에 따라 메시지 모양이 4가지로 갈리기 때문에** 이런 방어 코드가 생겼습니다.

주석에도 불확실함이 드러납니다: *"If the SMT is configured to unwrap, 'payload' might be the root."*

> 📌 **이건 코드의 문제가 아니라 인프라 설정이 통일되지 않은 결과입니다.**
> 커넥터 4개의 `schemas.enable`과 `expand.json.payload`를 같게 맞추면
> 소비자 코드에서 이 분기가 전부 사라집니다.
> 자세한 비교는 [08-infrastructure.md](08-infrastructure.md#3-커넥터-설정이-서비스마다-다르다)에 있습니다.

---

## 8. 그 외 눈여겨볼 점

### 8-1. 상품 삭제 전 장바구니 확인

```java
public void deleteProduct(Long productId) {
    Product product = productRepository.findById(productId)...;
    if (product.getStatus() == ProductStatus.DELETED) {
        throw new IllegalStateException("이미 삭제된 상품입니다. 상품 ID: " + productId);
    }
    // 장바구니에 담긴 상품인지 확인
    List<Long> skuIds = product.getSkus().stream().map(ProductSKU::getId).toList();
    if (!skuIds.isEmpty() && cartItemRepository.existsByProductSkuIdIn(skuIds)) {
        throw new IllegalStateException("장바구니에 담긴 상품은 삭제할 수 없습니다.");
    }
    // 소프트 삭제
}
```

**Soft Delete + 사전 검증** 조합입니다. 상품이 사라지면 장바구니에 유령 항목이 생기므로
아예 삭제를 막습니다. 사용자 경험을 고려한 판단입니다.

⚠️ 다만 **장바구니는 Redis가 1차 저장소입니다.** `cartItemRepository`(DB)를 확인하므로
**최근 10초 안에 담긴 상품은 감지하지 못합니다.**
Write-Behind 구조 때문에 생긴 사각지대이고, Redis도 함께 확인해야 정확합니다.

또한 상품 상태 검증이 주문 시점에도 있으므로([4-5](#4-5-createdirectorder의-검증-순서))
실질적인 피해는 제한적입니다.

### 8-2. 마이페이지 전용 엔드포인트

`domain/mypage/`는 **user-service가 Feign으로 호출하는 내부 API**입니다.

```java
// user-service/client/StoreServiceClient.java
@GetMapping("/api/v1/mypage/wishlists")
Page<WishlistResponse> getWishlistsByUserId(@RequestParam("userId") Long userId, ...);
```

**내부 호출용 API를 별도 패키지로 분리한 것은 좋은 구조입니다.**
일반 API(`/api/v1/orders`)는 `X-User-Id` 헤더로 사용자를 식별하고,
내부 API(`/api/v1/mypage/orders`)는 `userId`를 파라미터로 받습니다.

⚠️ **그런데 이것이 보안 구멍이 될 수 있습니다.**

```bash
# 게이트웨이 라우팅에 /api/mypage/** 가 없으므로 외부에서 직접 오지는 않는다.
# 하지만 서비스 포트가 열려 있으면:
curl "http://<node-4>:8085/api/v1/mypage/orders?userId=1"    # 남의 주문 조회
```

일반 API는 헤더를 게이트웨이가 덮어쓰므로 위조가 막히지만,
**내부 API는 `userId`를 그냥 받으므로 아무 값이나 넣을 수 있습니다.**
[02-api-gateway.md의 게이트웨이 우회 문제](02-api-gateway.md#6-1-게이트웨이-우회--헤더를-무조건-믿는다)와
결합하면 실제 위험이 됩니다.

**내부 API는 별도 포트나 공유 시크릿 헤더로 보호해야 합니다.**

### 8-3. Redis 랭킹

```java
private void updateRanking(Order order) {
    String today = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);   // yyyyMMdd
    ... redisTemplate.opsForZSet() ...
}
```

Redis의 **Sorted Set(ZSet)** 으로 일별 판매 랭킹을 만듭니다.
`ZINCRBY`로 점수를 올리고 `ZREVRANGE`로 상위 N개를 가져오는 방식은
"인기 상품 TOP 10"에 가장 적합한 자료구조입니다. DB에서 `GROUP BY ... ORDER BY COUNT DESC`를
매번 돌리는 것보다 압도적으로 빠릅니다.

⚠️ 다만 [7-5](#7-5-멱등성이-상태-검사에만-의존한다)에서 지적한 대로,
중복 처리 시 점수가 두 번 올라갑니다. 그리고 Redis가 초기화되면 랭킹이 사라지므로
TTL 정책과 복구 방법(DB에서 재계산)이 필요합니다.

### 8-4. 관리자 API가 게이트웨이에서만 막혀 있다

`GET /api/admin/orders`, `PATCH /api/admin/orders/{id}/status` 같은 API가 있고,
인가는 게이트웨이가 담당합니다.

**관리자 주문 API는 실제로 안전합니다.** config repo의 `admin-orders` 라우트가
`/api/v1/admin/orders`로 리라이트한 뒤 필터를 타므로
[`/api/v1/admin/**` 접두사 규칙](02-api-gateway.md#3-7-관리자-경로만-별도-처리)에 걸립니다.

⚠️ **반면 상품·카테고리·옵션 관리 API는 뚫려 있습니다.**
[02-api-gateway.md 6-2](02-api-gateway.md#6-2-인가-목록과-경로-리라이트가-엇갈려서-생긴-실제-구멍)에서
확인한 대로, `ADMIN_ONLY_PATHS`에 `POST:/api/products`로 등록됐지만
필터가 보는 경로는 리라이트된 `/api/v1/products`라서 매칭되지 않습니다.

→ **로그인한 일반 회원이 상품을 등록·수정·삭제할 수 있습니다.**
store-service 쪽에서 `X-User-Role`을 확인하면 게이트웨이 수정 없이도 막을 수 있습니다.

```java
@PostMapping
public ResponseEntity<?> create(@RequestHeader("X-User-Role") String role, ...) {
    if (!"ROLE_ADMIN".equals(role)) throw new AccessDeniedException("관리자 권한이 필요합니다.");
    ...
}
```

### 8-5. 주석의 언어와 완성도가 섞여 있다

```java
// 2. Sort Items by SKU ID to prevent Deadlock (Resource Ordering)
// Note: In @Transactional, if exception occurs, DB changes rollback.
// Optimization: Batch Fetch SKUs before loop or fetch here?
// Re-fetch SKU entities or use simple references?
```

영어 주석이고, **물음표로 끝나는 주석**이 있습니다. 작업 중의 고민이 그대로 남은 것입니다.
다른 서비스는 한국어 주석이 대부분이라 대비됩니다.

`List<ProductSKU> lockedSkus = new ArrayList<>();`도 **선언만 되고 쓰이지 않습니다**
(주석에 `// To track what we processed (implied by execution flow)`라고 적혀 있습니다).
이런 흔적은 리뷰 때 정리하는 게 좋습니다.

---

## 9. 이 문서에서 배울 개념 정리

| 개념 | 한 줄 설명 | 코드 위치 |
|---|---|---|
| **도메인별 패키지 구조** | 도메인이 많으면 계층별보다 낫다 | `domain/` |
| **SKU** | 재고와 가격은 옵션 조합 단위로 관리한다 | `ProductSKU` |
| **주문 스냅샷** | 영수증은 나중에 변하면 안 된다 | `OrderItem` ★ |
| **UUID 외부 식별자** | auto increment는 추측 가능하다 | `orderUuid` |
| **분산 락** | `synchronized`는 서버 1대에서만 유효하다 | Redisson |
| **`tryLock` vs `lock`** | 무한 대기는 스레드 고갈을 부른다 | `WAIT_TIME` |
| **`LEASE_TIME`** | 락을 쥔 서버가 죽어도 풀리게 한다 | `LEASE_TIME` |
| **`isHeldByCurrentThread`** | 남의 락을 풀지 않기 위한 확인 | `finally` |
| **자원 순서화** | 항상 같은 순서로 잠그면 데드락이 없다 | `cartItems.sort` |
| **🚨 락 > 트랜잭션** | 락은 커밋을 감싸야 한다. 안 그러면 무의미 | 4-4 ★ |
| **원자적 UPDATE** | 재고 차감은 DB에게 맡기는 게 가장 안전 | 4-4 (A) |
| **비관적 락** | 락 범위와 트랜잭션 범위가 자동 일치 | 4-4 (B) |
| **Cache-Aside** | 읽기는 캐시 우선, 쓰기는 캐시 무효화 | `ProductFacade` |
| **캐시 스탬피드** | 만료 순간 동시 요청이 DB를 덮친다 | 5-2 ★ |
| **Double-Check** | 락을 얻은 뒤 캐시를 다시 본다 | 5-2 |
| **Jitter TTL** | 만료 시각을 흩뿌려 부하를 평탄화 | 5-3 ★ |
| **Write-Behind** | 캐시에 먼저 쓰고 나중에 DB 반영 | `CartSyncScheduler` |
| **Dirty Set** | 바뀐 대상만 모아 중복 없이 처리 | `RSet` |
| **`fixedDelay` vs `fixedRate`** | 겹치면 안 되는 작업은 `fixedDelay` | 6-3 |
| **⚠️ 트랜잭션 밖의 작업** | Redis 제거·S3 삭제는 커밋 후에 | 6-4 ★ |
| **자기 호출과 프록시** | 같은 클래스 메서드 호출엔 `@Transactional`이 안 걸린다 | 6-4 |
| **Kafka 이벤트 유실** | 예외를 삼키면 오프셋이 커밋된다 | 🚨 7-3 ★ |
| **DLT** | 돈이 걸린 이벤트는 버리지 말고 격리한다 | 7-3 |
| **조건부 UPDATE 멱등성** | `WHERE status = 'PENDING'`이 중복을 막는다 | 7-5 |
| **Redis Sorted Set** | 랭킹은 ZSet이 정석 | 8-3 |

---

## 10. 개선 우선순위

| 순위 | 항목 | 심각도 | 근거 |
|---|---|---|---|
| 1 | 🚨 결제 이벤트 예외를 삼키지 말고 재시도/DLT | **매우 높음** | 결제 완료가 주문에 반영되지 않고 유실 (7-3) |
| 2 | 🚨 재고 차감을 원자적 UPDATE로 (또는 락 범위 수정) | **매우 높음** | 분산 락이 초과 판매를 막지 못한다 (4-4) |
| 3 | `CartSyncScheduler`를 사용자별 트랜잭션으로 분리 | **높음** | 한 명 실패가 전원 유실을 만든다 (6-4) |
| 4 | `LEASE_TIME` > 최악 처리 시간, `WAIT_TIME`보다 크게 | 중간 | 작업 중 락 자동 해제 (4-2, 5-4) |
| 5 | 내부 API(`/mypage/**`)에 접근 제어 추가 | 중간 | `userId` 파라미터를 그대로 신뢰 (8-2) |
| 6 | 상품·카테고리 관리 API에 역할 확인 추가 | 중간 | 게이트웨이 인가가 매칭되지 않는다 (8-4) |
| 7 | `cancelOrder`를 조건부 UPDATE로 | 중간 | 동시 취소 시 재고가 두 번 복구될 수 있다 (4-6) |
| 8 | `completeOrder` 멱등성을 조건부 UPDATE로 | 중간 | 랭킹 점수 이중 반영 (7-5) |
| 9 | `payment.events` / `payment` 토픽 하나로 정리 | 낮음 | 중복 처리 가능성 (7-4) |
| 10 | `createDirectOrder`의 검증 순서를 `createOrder`와 통일 | 낮음 | 불필요한 락·쓰기 (4-5) |
| 11 | 캐시 락에 watchdog 사용 + 빈 try-catch 제거 | 낮음 | 스탬피드 방지가 풀린다, 죽은 코드 (5-4) |
| 12 | 장바구니 조회 시 사라진 SKU를 Redis에서 정리 | 낮음 | 유령 항목 누적 (6-6) |
| 13 | `store.outbox.events` 소비자 확인 또는 주석 | 낮음 | 발행만 하고 쓰지 않는다 (7-2) |
| 14 | 미사용 변수·물음표 주석 정리 | 낮음 | `lockedSkus` 등 (8-5) |

---

**다음 문서** → [07-payment-service.md](07-payment-service.md) — 토스 결제 연동과 가장 작은 서비스의 가장 큰 위험
