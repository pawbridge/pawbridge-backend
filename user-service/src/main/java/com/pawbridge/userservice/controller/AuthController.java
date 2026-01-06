package com.pawbridge.userservice.controller;

import com.pawbridge.userservice.dto.request.PasswordResetRequestDto;
import com.pawbridge.userservice.dto.request.PasswordResetVerifyDto;
import com.pawbridge.userservice.service.AuthService;
import com.pawbridge.userservice.util.ResponseDTO;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/auth")
public class AuthController {

    private final AuthService authService;

    /**
     * 토큰 재발급
     */
    @PostMapping("/refresh")
    public ResponseEntity<ResponseDTO<Void>> refreshToken(
            HttpServletRequest request,
            HttpServletResponse response) {

        authService.refreshToken(request, response);
        ResponseDTO<Void> responseDTO = ResponseDTO.okWithMessage("토큰이 재발급되었습니다.");
        return ResponseEntity
                .status(responseDTO.getCode())
                .body(responseDTO);
    }

    /**
     * 로그아웃
     */
    @PostMapping("/logout")
    public ResponseEntity<ResponseDTO<Void>> logout(
            @RequestHeader("X-User-Id") Long userId,
            HttpServletResponse response) {

        authService.logout(userId, response);
        ResponseDTO<Void> responseDTO = ResponseDTO.okWithMessage("로그아웃되었습니다.");
        return ResponseEntity
                .status(responseDTO.getCode())
                .body(responseDTO);
    }

    /**
     * 비밀번호 재설정 - 인증코드 발송
     */
    @PostMapping("/password/reset-request")
    public ResponseEntity<ResponseDTO<Void>> requestPasswordReset(
            @Valid @RequestBody PasswordResetRequestDto requestDto) {

        authService.requestPasswordReset(requestDto);
        ResponseDTO<Void> response = ResponseDTO.okWithMessage(
                "인증코드가 이메일로 발송되었습니다.");
        return ResponseEntity
                .status(response.getCode())
                .body(response);
    }

    /**
     * 비밀번호 재설정 - 인증 및 새 비밀번호 설정
     */
    @PostMapping("/password/reset")
    public ResponseEntity<ResponseDTO<Void>> resetPassword(
            @Valid @RequestBody PasswordResetVerifyDto requestDto) {

        authService.resetPassword(requestDto);
        ResponseDTO<Void> response = ResponseDTO.okWithMessage(
                "비밀번호가 재설정되었습니다.");
        return ResponseEntity
                .status(response.getCode())
                .body(response);
    }
}
