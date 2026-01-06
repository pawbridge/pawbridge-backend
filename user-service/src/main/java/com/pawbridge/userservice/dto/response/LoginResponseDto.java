package com.pawbridge.userservice.dto.response;

import com.pawbridge.userservice.entity.User;

public record LoginResponseDto(
        Long userId,
        String email,
        String name,
        String role,        // 추가
        String careRegNo   // 추가 (ROLE_SHELTER인 경우만)
) {
    public static LoginResponseDto fromEntity(User user) {
        return new LoginResponseDto(
                user.getUserId(),
                user.getEmail(),
                user.getName(),
                user.getRole().name(),  // Role enum을 String으로 변환
                user.getCareRegNo()    // null일 수 있음 (ROLE_USER인 경우)
        );
    }
}
