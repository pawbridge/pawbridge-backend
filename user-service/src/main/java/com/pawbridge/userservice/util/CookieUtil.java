package com.pawbridge.userservice.util;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Optional;

/**
 * 쿠키 유틸리티
 * - HttpOnly 쿠키 생성/삭제
 * - 환경별 Domain, Secure 플래그 자동 설정
 */
@Component
public class CookieUtil {

    public static final String ACCESS_TOKEN_COOKIE = "accessToken";
    public static final String REFRESH_TOKEN_COOKIE = "refreshToken";

    // Access Token 만료 시간 (30분 = 1800초)
    private static final int ACCESS_TOKEN_MAX_AGE = 30 * 60;

    // Refresh Token 만료 시간 (7일 = 604800초)
    private static final int REFRESH_TOKEN_MAX_AGE = 7 * 24 * 60 * 60;

    @Value("${spring.profiles.active:local}")
    private String activeProfile;

    /**
     * Access Token 쿠키 생성
     */
    public void createAccessTokenCookie(HttpServletResponse response, String accessToken) {
        createCookie(response, ACCESS_TOKEN_COOKIE, accessToken, ACCESS_TOKEN_MAX_AGE);
    }

    /**
     * Refresh Token 쿠키 생성
     */
    public void createRefreshTokenCookie(HttpServletResponse response, String refreshToken) {
        createCookie(response, REFRESH_TOKEN_COOKIE, refreshToken, REFRESH_TOKEN_MAX_AGE);
    }

    /**
     * 쿠키 생성 (공통 로직)
     */
    private void createCookie(HttpServletResponse response, String name, String value, int maxAge) {
        boolean isProduction = "prod".equals(activeProfile);

        StringBuilder cookieBuilder = new StringBuilder();
        cookieBuilder.append(name).append("=").append(value);
        cookieBuilder.append("; Path=/");
        cookieBuilder.append("; Max-Age=").append(maxAge);
        cookieBuilder.append("; HttpOnly");

        if (isProduction) {
            // 운영: Domain, SameSite=None, Secure
            cookieBuilder.append("; Domain=.pawbridge.kr");
            cookieBuilder.append("; SameSite=None");
            cookieBuilder.append("; Secure");
        } else {
            // 개발: SameSite 생략 (기본 Lax, localhost는 자동 작동)
            // SameSite=None은 Secure 필요하므로 생략
        }

        response.addHeader("Set-Cookie", cookieBuilder.toString());
    }

    /**
     * Access Token 쿠키 삭제
     */
    public void deleteAccessTokenCookie(HttpServletResponse response) {
        deleteCookie(response, ACCESS_TOKEN_COOKIE);
    }

    /**
     * Refresh Token 쿠키 삭제
     */
    public void deleteRefreshTokenCookie(HttpServletResponse response) {
        deleteCookie(response, REFRESH_TOKEN_COOKIE);
    }

    /**
     * 쿠키 삭제 (공통 로직)
     */
    private void deleteCookie(HttpServletResponse response, String name) {
        boolean isProduction = "prod".equals(activeProfile);

        StringBuilder cookieBuilder = new StringBuilder();
        cookieBuilder.append(name).append("=");
        cookieBuilder.append("; Path=/");
        cookieBuilder.append("; Max-Age=0");
        cookieBuilder.append("; HttpOnly");

        if (isProduction) {
            cookieBuilder.append("; Domain=.pawbridge.kr");
            cookieBuilder.append("; SameSite=None");
            cookieBuilder.append("; Secure");
        }

        response.addHeader("Set-Cookie", cookieBuilder.toString());
    }

    /**
     * 요청에서 특정 쿠키 값 추출
     */
    public Optional<String> getCookieValue(HttpServletRequest request, String name) {
        if (request.getCookies() == null) {
            return Optional.empty();
        }

        return Arrays.stream(request.getCookies())
                .filter(cookie -> name.equals(cookie.getName()))
                .map(Cookie::getValue)
                .findFirst();
    }

    /**
     * Access Token 쿠키 값 추출
     */
    public Optional<String> getAccessToken(HttpServletRequest request) {
        return getCookieValue(request, ACCESS_TOKEN_COOKIE);
    }

    /**
     * Refresh Token 쿠키 값 추출
     */
    public Optional<String> getRefreshToken(HttpServletRequest request) {
        return getCookieValue(request, REFRESH_TOKEN_COOKIE);
    }
}
