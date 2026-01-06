package com.pawbridge.apigateway.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pawbridge.apigateway.util.ErrorResponse;
import com.pawbridge.apigateway.util.JwtUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * JWT 인가 필터
 * - Access Token 검증
 * - 사용자 정보 추출 및 헤더 추가
 */
@Slf4j
@Component
public class JwtAuthorizationGatewayFilterFactory
        extends AbstractGatewayFilterFactory<JwtAuthorizationGatewayFilterFactory.Config> {

    private final JwtUtil jwtUtil;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AntPathMatcher pathMatcher = new AntPathMatcher();

    // 토큰 검증이 필요 없는 경로 (화이트리스트)
    private static final List<String> WHITELIST = List.of(
            "/api/v1/users/signup",              // 회원가입
            "/api/v1/auth/login",                 // 로그인
            "/api/v1/auth/refresh",               // 토큰 재발급 (Access Token 만료 시 사용)
            "/api/v1/auth/password/reset-request", // 비밀번호 재설정 요청
            "/api/v1/auth/password/reset",         // 비밀번호 재설정
            "/api/v1/posts/read/*",                // 게시글 단건 조회
            "/api/v1/posts/read",                   // 게시글 목록 조회
            "/api/v1/posts/search",                 // 검색
            "/api/v1/comments/posts/read/*"        // 특정 게시글 댓글 목록 조회
    );

    // ROLE_ADMIN만 접근 가능한 경로 (프론트엔드 요청 기준, v1 없음)
    private static final List<String> ADMIN_ONLY_PATHS = List.of(
            "POST:/api/v1/shelters",               // 보호소 등록
            "DELETE:/api/v1/shelters/*",           // 보호소 삭제
            // Store Service 관리자 API (프론트 요청 기준)
            "POST:/api/products",                  // 상품 등록
            "PATCH:/api/products/*",               // 상품 수정
            "DELETE:/api/products/*",              // 상품 삭제
            "POST:/api/categories",                // 카테고리 등록
            "PUT:/api/categories/*",               // 카테고리 수정
            "DELETE:/api/categories/*",            // 카테고리 삭제
            "POST:/api/option-groups",             // 옵션 그룹 등록
            "PUT:/api/option-groups/*",            // 옵션 그룹 수정
            "DELETE:/api/option-groups/*",         // 옵션 그룹 삭제
            "POST:/api/option-groups/*/values",    // 옵션 값 추가
            "PUT:/api/option-groups/values/*",     // 옵션 값 수정
            "DELETE:/api/option-groups/values/*",  // 옵션 값 삭제
            "GET:/api/admin/orders",               // 관리자 주문 목록
            "PATCH:/api/admin/orders/*/status",    // 주문 상태 변경
            "PATCH:/api/admin/orders/*/delivery-status"  // 배송 상태 변경
    );

    // ROLE_USER가 아닐 때 접근 가능한 경로 (ROLE_ADMIN, ROLE_SHELTER)
    private static final List<String> NON_USER_PATHS = List.of(
            "PUT:/api/v1/shelters/*",              // 보호소 수정
            "PATCH:/api/v1/shelters/*",            // 보호소 부분 수정
            "POST:/api/v1/animals",                // 동물 등록
            "POST:/api/v1/animals/*",              // 동물 등록 (하위 경로)
            "PUT:/api/v1/animals/*",               // 동물 수정
            "PATCH:/api/v1/animals/*",             // 동물 부분 수정
            "DELETE:/api/v1/animals/*"             // 동물 삭제
    );

    public JwtAuthorizationGatewayFilterFactory(JwtUtil jwtUtil) {
        super(Config.class);
        this.jwtUtil = jwtUtil;
    }

    @Override
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            ServerHttpRequest request = exchange.getRequest();
            String path = request.getURI().getPath();

            // 화이트리스트 경로는 토큰 검증 스킵
            if (isWhitelisted(path)) {
                log.info("화이트리스트 경로: {}", path);
                return chain.filter(exchange);
            }

            // 쿠키에서 토큰 추출
            String token = extractTokenFromCookie(request);

            if (token == null) {
                log.warn("쿠키에 토큰 없음: {}", path);
                return onError(exchange, "인증 토큰이 필요합니다.", HttpStatus.UNAUTHORIZED);
            }

            // Access Token 검증
            if (!jwtUtil.validateAccessToken(token)) {
                log.warn("유효하지 않은 토큰: {}", path);
                return onError(exchange, "유효하지 않거나 만료된 토큰입니다.", HttpStatus.UNAUTHORIZED);
            }

            // 토큰에서 사용자 정보 추출
            try {
                Long userId = jwtUtil.getUserIdFromToken(token);
                String email = jwtUtil.getEmailFromToken(token);
                String name = jwtUtil.getNameFromToken(token);
                String role = jwtUtil.getRoleFromToken(token);
                String method = request.getMethod().name();

                // Role 기반 접근 제어
                // 1. ADMIN만 접근 가능한 경로 체크
                if (isAdminOnlyPath(method, path) && !role.equals("ROLE_ADMIN")) {
                    log.warn("관리자 전용 경로 접근 거부 - role: {}, path: {}", role, path);
                    return onError(exchange, "관리자 권한이 필요합니다.", HttpStatus.FORBIDDEN);
                }

                // 2. ROLE_USER가 아닐 때 접근 가능한 경로 체크 (ROLE_ADMIN, ROLE_SHELTER만 가능)
                if (isNonUserPath(method, path) && role.equals("ROLE_USER")) {
                    log.warn("권한 부족 - role: {}, path: {}", role, path);
                    return onError(exchange, "권한이 부족합니다.", HttpStatus.FORBIDDEN);
                }

                // Authorization 헤더는 유지하고, X-User-* 헤더 추가
                // (외부 서비스 호출 시 원본 토큰 필요할 수 있음)
                var requestBuilder = request.mutate()
                        .header("X-User-Id", userId.toString())
                        .header("X-User-Email", email)
                        .header("X-User-Name", name)
                        .header("X-User-Role", role);

                // ROLE_SHELTER인 경우 careRegNo 헤더 추가
                if ("ROLE_SHELTER".equals(role)) {
                    String careRegNo = jwtUtil.getCareRegNoFromToken(token);
                    if (careRegNo != null && !careRegNo.isBlank()) {
                        requestBuilder.header("X-Care-Reg-No", careRegNo);
                        log.info("JWT 검증 성공 - userId: {}, email: {}, role: {}, careRegNo: {}, path: {}",
                                userId, email, role, careRegNo, path);
                    } else {
                        log.info("JWT 검증 성공 - userId: {}, email: {}, role: {}, path: {}", userId, email, role, path);
                    }
                } else {
                    log.info("JWT 검증 성공 - userId: {}, email: {}, role: {}, path: {}", userId, email, role, path);
                }

                ServerHttpRequest modifiedRequest = requestBuilder.build();

                return chain.filter(exchange.mutate().request(modifiedRequest).build());

            } catch (Exception e) {
                log.error("토큰 파싱 실패: {}", e.getMessage());
                return onError(exchange, "토큰 파싱에 실패했습니다.", HttpStatus.UNAUTHORIZED);
            }
        };
    }

    /**
     * 쿠키에서 accessToken 추출 (WebFlux 방식)
     */
    private String extractTokenFromCookie(ServerHttpRequest request) {
        var cookies = request.getCookies();
        var tokenCookies = cookies.get("accessToken");

        if (tokenCookies != null && !tokenCookies.isEmpty()) {
            return tokenCookies.get(0).getValue();
        }

        return null;
    }

    /**
     * 화이트리스트 경로 확인 (와일드카드 지원)
     */
    private boolean isWhitelisted(String path) {
        return WHITELIST.stream()
                .anyMatch(pattern -> pathMatcher.match(pattern, path));
    }

    /**
     * ADMIN만 접근 가능한 경로인지 확인
     */
    private boolean isAdminOnlyPath(String method, String path) {
        // /api/v1/admin/** 패턴은 모든 HTTP 메서드에 대해 ADMIN 권한 필요
        // (RewritePath 필터 후 변환된 경로를 체크)
        if (pathMatcher.match("/api/v1/admin/**", path)) {
            return true;
        }

        return ADMIN_ONLY_PATHS.stream()
                .anyMatch(pattern -> matchesMethodAndPath(pattern, method, path));
    }

    /**
     * ROLE_USER가 아닐 때 접근 가능한 경로인지 확인
     */
    private boolean isNonUserPath(String method, String path) {
        return NON_USER_PATHS.stream()
                .anyMatch(pattern -> matchesMethodAndPath(pattern, method, path));
    }

    /**
     * HTTP 메서드와 경로를 함께 매칭 (와일드카드 지원)
     * @param pattern "METHOD:/path/pattern" 형식
     * @param method 요청 HTTP 메서드
     * @param path 요청 경로
     */
    private boolean matchesMethodAndPath(String pattern, String method, String path) {
        String[] parts = pattern.split(":", 2);
        if (parts.length != 2) {
            return false;
        }
        String patternMethod = parts[0];
        String patternPath = parts[1];
        return patternMethod.equals(method) && pathMatcher.match(patternPath, path);
    }

    /**
     * 에러 응답 (JSON 형식)
     */
    private Mono<Void> onError(ServerWebExchange exchange, String message, HttpStatus status) {
        ServerHttpResponse response = exchange.getResponse();
        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        log.error("JWT 인가 실패 - message: {}, status: {}, path: {}",
                message, status, exchange.getRequest().getURI().getPath());

        // 다른 서비스의 ResponseDTO와 동일한 구조로 에러 응답 생성
        ErrorResponse errorResponse = ErrorResponse.of(status.value(), message);

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(errorResponse);
            DataBuffer buffer = response.bufferFactory().wrap(bytes);
            return response.writeWith(Mono.just(buffer));
        } catch (JsonProcessingException e) {
            log.error("JSON 변환 실패: {}", e.getMessage());
            return response.setComplete();
        }
    }

    /**
     * 설정 클래스
     */
    public static class Config {
        // 필요시 설정값 추가
    }
}
