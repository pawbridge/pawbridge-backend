package com.pawbridge.userservice.service;

import com.pawbridge.userservice.email.service.EmailVerificationService;
import com.pawbridge.userservice.dto.request.PasswordResetRequestDto;
import com.pawbridge.userservice.dto.request.PasswordResetVerifyDto;
import com.pawbridge.userservice.entity.RefreshToken;
import com.pawbridge.userservice.entity.User;
import com.pawbridge.userservice.exception.PasswordResetCodeInvalidException;
import com.pawbridge.userservice.exception.RefreshTokenExpiredException;
import com.pawbridge.userservice.exception.RefreshTokenNotFoundException;
import com.pawbridge.userservice.exception.TokenInvalidException;
import com.pawbridge.userservice.exception.UserNotFoundException;
import com.pawbridge.userservice.jwt.JwtProvider;
import com.pawbridge.userservice.repository.RefreshTokenRepository;
import com.pawbridge.userservice.repository.UserRepository;
import com.pawbridge.userservice.util.CookieUtil;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class AuthServiceImpl implements AuthService {

    private final RefreshTokenRepository refreshTokenRepository;
    private final UserRepository userRepository;
    private final JwtProvider jwtProvider;
    private final PasswordEncoder passwordEncoder;
    private final EmailVerificationService emailVerificationService;
    private final CookieUtil cookieUtil;

    /**
     * Refresh Token을 사용하여 새로운 Access Token과 Refresh Token 발급
     */
    @Override
    @Transactional
    public void refreshToken(HttpServletRequest request, HttpServletResponse response) {
        // 1. 쿠키에서 Refresh Token 추출
        String refreshTokenValue = cookieUtil.getRefreshToken(request)
                .orElseThrow(RefreshTokenNotFoundException::new);

        // 2. Refresh Token JWT 유효성 검증
        if (!jwtProvider.validateRefreshToken(refreshTokenValue)) {
            throw new TokenInvalidException();
        }

        // 3. DB에서 Refresh Token 조회
        RefreshToken refreshToken = refreshTokenRepository.findByToken(refreshTokenValue)
                .orElseThrow(RefreshTokenNotFoundException::new);

        // 4. Refresh Token 만료 여부 확인
        if (refreshToken.isExpired()) {
            refreshTokenRepository.delete(refreshToken);
            throw new RefreshTokenExpiredException();
        }

        // 5. 사용자 정보 조회
        User user = userRepository.findById(refreshToken.getUserId())
                .orElseThrow(UserNotFoundException::new);

        // 6. 새로운 Access Token 생성
        String newAccessToken = jwtProvider.createAccessToken(user);

        // 7. 새로운 Refresh Token 생성
        String newRefreshToken = jwtProvider.createRefreshToken();

        // 8. DB의 Refresh Token 업데이트
        long refreshTokenExpirationMs = jwtProvider.getRefreshTokenExpiration();
        LocalDateTime newExpiresAt = LocalDateTime.now()
                .plusSeconds(TimeUnit.MILLISECONDS.toSeconds(refreshTokenExpirationMs));

        refreshToken.updateToken(newRefreshToken, newExpiresAt);
        refreshTokenRepository.save(refreshToken);

        // 9. 쿠키에 새 토큰 설정
        cookieUtil.createAccessTokenCookie(response, newAccessToken);
        cookieUtil.createRefreshTokenCookie(response, newRefreshToken);
    }

    /**
     * 로그아웃 - Refresh Token 삭제
     */
    @Override
    @Transactional
    public void logout(Long userId, HttpServletResponse response) {
        // DB에서 Refresh Token 삭제
        refreshTokenRepository.deleteByUserId(userId);

        // 쿠키 삭제
        cookieUtil.deleteAccessTokenCookie(response);
        cookieUtil.deleteRefreshTokenCookie(response);
    }

    /**
     * 비밀번호 재설정 요청 (이메일 발송)
     */
    @Override
    @Transactional(readOnly = true)
    public void requestPasswordReset(PasswordResetRequestDto requestDto) {
        // LOCAL 사용자만 조회 (OAuth2 사용자 제외)
        Optional<User> userOpt = userRepository.findByEmailAndProvider(
                requestDto.getEmail(), "LOCAL");

        if (userOpt.isPresent()) {
            try {
                // 이메일 발송
                emailVerificationService.sendPasswordResetCode(requestDto.getEmail());
                log.info("비밀번호 재설정 이메일 발송 성공: {}", requestDto.getEmail());
            } catch (Exception e) {
                log.error("이메일 발송 실패: {}", e.getMessage());
                // 보안상 실패해도 성공 응답 (계정 존재 여부 노출 방지)
            }
        }
        // 이메일이 없어도 동일하게 성공 응답 (보안)
        log.debug("비밀번호 재설정 요청 처리 완료: {}", requestDto.getEmail());
    }

    /**
     * 비밀번호 재설정 (인증 후 변경)
     */
    @Override
    @Transactional
    public void resetPassword(PasswordResetVerifyDto requestDto) {
        // 1. LOCAL 사용자만 조회 (보안: 존재하지 않아도 동일한 예외)
        User user = userRepository.findByEmailAndProvider(
                requestDto.getEmail(), "LOCAL")
                .orElseThrow(() -> new PasswordResetCodeInvalidException());

        // 2. 인증코드 검증
        try {
            boolean verified = emailVerificationService.verifyPasswordResetCode(
                    requestDto.getEmail(),
                    requestDto.getCode());

            if (!verified) {
                throw new PasswordResetCodeInvalidException();
            }
        } catch (Exception e) {
            log.error("인증코드 검증 실패: {}", e.getMessage());
            throw new PasswordResetCodeInvalidException();
        }

        // 3. 비밀번호 암호화 및 변경
        String encodedPassword = passwordEncoder.encode(requestDto.getNewPassword());
        user.updatePassword(encodedPassword);
        userRepository.save(user);

        // 4. 인증 정보 삭제 (실패해도 계속 진행)
        try {
            emailVerificationService.clearPasswordResetVerification(requestDto.getEmail());
        } catch (Exception e) {
            log.warn("인증 정보 삭제 실패 (무시): {}", e.getMessage());
        }

        log.info("비밀번호 재설정 완료: {}", requestDto.getEmail());
    }
}
