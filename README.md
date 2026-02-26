# PawBridge Backend (EC2 Edition)

> ⚠️ **이 레포지토리는 AWS EC2 배포 버전 (아카이브)입니다.**
> 
> 현재 활발히 개발 중인 **Kubernetes 버전**은 아래에서 확인할 수 있습니다.
> - 📦 [pawbridge-backend-k8s](https://github.com/pawbridge/pawbridge-backend-k8s) — 백엔드 (K8s)
> - 🏗️ [pawbridge-infra-k8s](https://github.com/pawbridge/pawbridge-infra-k8s) — 인프라 (K8s)

---

## 프로젝트 소개

**PawBridge**는 유기동물 정보 제공, 입양 매칭, 커뮤니티, 후원 스토어를 통합한 반려동물 보호 플랫폼입니다.

**주요 기능:**
- 유기동물 조회/검색 (농림축산식품부 APMS 공공데이터 연동)
- 사용자 인증 (일반/OAuth2 소셜 로그인)
- 커뮤니티 게시판
- 후원 스토어 (상품/결제 - 토스페이먼츠 연동)

---

## 기술 스택

| 분야 | 기술 |
|------|------|
| Backend | Java 17, Spring Boot 3.4, Spring Cloud |
| Database | MySQL 8.0, Elasticsearch 8.11, Redis 7 |
| Messaging | Apache Kafka, Debezium CDC |
| Infra | AWS EC2 (5-Node), Docker |
| CI/CD | GitHub Actions |
| Monitoring | Prometheus, Grafana, Zipkin |

---

## EC2 인프라 배치 (5-Node)

```
Internet → Nginx (Reverse Proxy) → API Gateway → Microservices
```

| 노드 | 컴포넌트 |
|------|----------|
| Node-1 | Gateway, Eureka, Redis, Monitoring |
| Node-2 | MySQL, Config Server |
| Node-3 | Kafka, Animal Service |
| Node-4 | User, Community, Store, Payment |
| Node-5 | Elasticsearch, Kafka Connect |

---

## 서비스 구성

| Service | Port | Description |
|---|---|---|
| api-gateway | 8080 | 공통 라우팅, JWT 인증/인가 필터링 |
| user-service | 8080 | 회원가입, 로그인, JWT 토큰 발급, 이메일 인증 |
| animal-service | 8081 | 공공데이터(APMS) 유기동물 배치 수집, 검색 |
| community-service | 8082 | 커뮤니티 피드, 게시글 및 댓글 |
| store-service | 8083 | 상품 관리, 장바구니, 검색 (Outbox 연동) |
| payment-service | 8084 | 토스페이먼츠 연동 결제 처리 |

---

## 주요 기술적 챌린지

### 1. 재고 동시성 제어 (Overselling 방지)
- **문제:** 인기 상품 주문 폭주 시 재고 음수 발생
- **해결:** 비관적 락 (PESSIMISTIC_WRITE) + SKU ID 순 정렬로 Deadlock 방지

### 2. 실시간 검색 동기화 (CDC)
- **문제:** MySQL과 Elasticsearch 간 데이터 불일치
- **해결:** Debezium CDC + Transactional Outbox Pattern으로 메시지 유실률 0% 달성

### 3. Saga 패턴 - 보상 트랜잭션
- **문제:** 분산 환경에서 서비스 실패 시 데이터 정합성 파괴
- **해결:** Kafka 기반 보상 이벤트 발행으로 실패 시 자동 롤백

### 4. 결제 금액 변조 방지
- **문제:** 프론트엔드 스크립트 조작으로 금액 위조 가능
- **해결:** 서버 측 이중 검증 (Double Verification)

### 5. 멀티 노드 Eureka IP 문제
- **문제:** Docker 내부 IP로 등록되어 노드 간 통신 불가
- **해결:** 환경변수로 노드 프라이빗 IP 명시

---

## K8s로 마이그레이션한 이유

본 EC2 배포 운영 경험을 바탕으로, 아래의 이유로 Kubernetes 기반 환경으로 마이그레이션하였습니다.

| 항목 | EC2 (기존) | K8s (현재) |
|---|---|---|
| 배포 방식 | GitHub Actions CI/CD 자동화 | Helm Chart 수동 배포 |
| 서비스 디스커버리 | Spring Cloud Eureka | K8s Native DNS (CoreDNS) |
| 설정 관리 | Spring Cloud Config Server | Helm Values + K8s Secret |
| 스케일링 | 수동 (EC2 증설) | 선언적 (ReplicaSet) |
| 비용 | AWS EC2 5대 월 과금 | 로컬 VM 무료 |
