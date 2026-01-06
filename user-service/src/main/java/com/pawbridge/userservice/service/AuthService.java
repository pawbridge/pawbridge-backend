package com.pawbridge.userservice.service;

import com.pawbridge.userservice.dto.request.PasswordResetRequestDto;
import com.pawbridge.userservice.dto.request.PasswordResetVerifyDto;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public interface AuthService {

    /**
     * Refresh Token을 사용하여 새로운 Access Token과 Refresh Token 발급
     */
    void refreshToken(HttpServletRequest request, HttpServletResponse response);

    /**
     * 로그아웃 - Refresh Token 삭제
     */
    void logout(Long userId, HttpServletResponse response);

    /**
     * 비밀번호 재설정 요청 (이메일 발송)
     */
    void requestPasswordReset(PasswordResetRequestDto requestDto);

    /**
     * 비밀번호 재설정 (인증 후 변경)
     */
    void resetPassword(PasswordResetVerifyDto requestDto);
}
