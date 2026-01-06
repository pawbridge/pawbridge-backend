package com.pawbridge.userservice.oauth2.handler;

import com.pawbridge.userservice.entity.RefreshToken;
import com.pawbridge.userservice.entity.User;
import com.pawbridge.userservice.jwt.JwtProvider;
import com.pawbridge.userservice.repository.RefreshTokenRepository;
import com.pawbridge.userservice.security.PrincipalDetails;
import com.pawbridge.userservice.util.CookieUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.SimpleUrlAuthenticationSuccessHandler;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * OAuth2 로그인 성공 시 처리 핸들러
 * JWT Access Token과 Refresh Token을 생성하고 프론트엔드로 리다이렉트
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OAuth2SuccessHandler extends SimpleUrlAuthenticationSuccessHandler {

    private final JwtProvider jwtProvider;
    private final RefreshTokenRepository refreshTokenRepository;
    private final CookieUtil cookieUtil;

    @Value("${oauth2.redirect-uri}")
    private String redirectUri;

    @Override
    public void onAuthenticationSuccess(
            HttpServletRequest request,
            HttpServletResponse response,
            Authentication authentication
    ) throws IOException {
        try {
            // 1. 사용자 정보 추출
            PrincipalDetails principalDetails = (PrincipalDetails) authentication.getPrincipal();
            User user = principalDetails.getUser();

            log.info("OAuth2 로그인 성공: userId={}, email={}, provider={}",
                    user.getUserId(), user.getEmail(), user.getProvider());

            // 2. JWT Access Token 생성 (기존 JwtProvider 재사용)
            String accessToken = jwtProvider.createAccessToken(user);

            // 3. Refresh Token 생성 (기존 JwtProvider 재사용)
            String refreshToken = jwtProvider.createRefreshToken();

            // 4. Refresh Token DB 저장 (JwtAuthenticationFilter와 동일한 패턴)
            saveRefreshToken(user.getUserId(), refreshToken);

            // 5. 쿠키에 토큰 설정
            cookieUtil.createAccessTokenCookie(response, accessToken);
            cookieUtil.createRefreshTokenCookie(response, refreshToken);

            // 6. 프론트엔드로 리다이렉트 (쿠키는 자동 전송)
            log.info("OAuth2 리다이렉트: {}", redirectUri);

            getRedirectStrategy().sendRedirect(request, response, redirectUri);

        } catch (Exception e) {
            log.error("OAuth2 토큰 생성 실패: {}", e.getMessage(), e);

            // 에러 페이지로 리다이렉트
            String errorMessage = URLEncoder.encode(
                    "로그인 처리 중 오류가 발생했습니다",
                    StandardCharsets.UTF_8
            );
            String errorUrl = redirectUri.replace("/oauth/callback", "/login") +
                    "?error=" + errorMessage;

            getRedirectStrategy().sendRedirect(request, response, errorUrl);
        }
    }

    /**
     * Refresh Token을 DB에 저장
     * JwtAuthenticationFilter와 동일한 로직 (기존 토큰 업데이트 또는 새로 생성)
     */
    private void saveRefreshToken(Long userId, String refreshToken) {
        long refreshTokenExpirationMs = jwtProvider.getRefreshTokenExpiration();
        LocalDateTime expiresAt = LocalDateTime.now()
                .plusSeconds(TimeUnit.MILLISECONDS.toSeconds(refreshTokenExpirationMs));

        refreshTokenRepository.findByUserId(userId)
                .ifPresentOrElse(
                        // 기존 토큰이 있으면 업데이트
                        existingToken -> {
                            existingToken.updateToken(refreshToken, expiresAt);
                            refreshTokenRepository.save(existingToken);
                            log.info("RefreshToken 업데이트: userId={}", userId);
                        },
                        // 없으면 새로 생성
                        () -> {
                            RefreshToken newRefreshToken = RefreshToken.builder()
                                    .token(refreshToken)
                                    .userId(userId)
                                    .expiresAt(expiresAt)
                                    .build();
                            refreshTokenRepository.save(newRefreshToken);
                            log.info("RefreshToken 생성: userId={}", userId);
                        }
                );
    }
}
