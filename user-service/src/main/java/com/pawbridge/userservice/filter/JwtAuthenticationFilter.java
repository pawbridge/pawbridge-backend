package com.pawbridge.userservice.filter;

import com.pawbridge.userservice.dto.request.LoginRequestDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pawbridge.userservice.dto.response.LoginResponseDto;
import com.pawbridge.userservice.entity.RefreshToken;
import com.pawbridge.userservice.entity.User;
import com.pawbridge.userservice.jwt.JwtProvider;
import com.pawbridge.userservice.repository.RefreshTokenRepository;
import com.pawbridge.userservice.security.PrincipalDetails;
import com.pawbridge.userservice.util.CookieUtil;
import com.pawbridge.userservice.util.CustomResponseUtil;
import com.pawbridge.userservice.util.ResponseDTO;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * JWT를 이용한 로그인 인증 필터
 */
public class JwtAuthenticationFilter extends UsernamePasswordAuthenticationFilter {

    private final JwtProvider jwtProvider;
    private final RefreshTokenRepository refreshTokenRepository;
    private final CookieUtil cookieUtil;

    public JwtAuthenticationFilter(
            AuthenticationManager authenticationManager,
            JwtProvider jwtProvider,
            RefreshTokenRepository refreshTokenRepository,
            CookieUtil cookieUtil
    ) {
        super.setAuthenticationManager(authenticationManager);
        this.jwtProvider = jwtProvider;
        this.refreshTokenRepository = refreshTokenRepository;
        this.cookieUtil = cookieUtil;
        // 로그인 엔드포인트 설정
        setFilterProcessesUrl("/api/v1/auth/login");
    }

    /**
     * 로그인 인증 시도
     */
    @Override
    public Authentication attemptAuthentication(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws AuthenticationException {
        try {
            // 요청 Body에서 email, password 추출
            ObjectMapper objectMapper = new ObjectMapper();
            LoginRequestDto loginRequest = objectMapper.readValue(
                    request.getInputStream(),
                    LoginRequestDto.class
            );

            // AuthenticationToken 생성
            UsernamePasswordAuthenticationToken authenticationToken =
                    new UsernamePasswordAuthenticationToken(
                            loginRequest.email(),
                            loginRequest.password(),
                            new ArrayList<>()
                    );

            // AuthenticationManager에게 인증 요청
            // 내부적으로 PrincipalDetailsService.loadUserByUsername() 호출
            // BCryptPasswordEncoder로 비밀번호 검증
            return this.getAuthenticationManager().authenticate(authenticationToken);

        } catch (IOException e) {
            throw new RuntimeException("로그인 요청 처리 중 오류가 발생했습니다.", e);
        }
    }

    /**
     * 인증 성공 시
     */
    @Override
    protected void successfulAuthentication(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain,
            Authentication authResult
    ) throws IOException, ServletException {
        // 인증된 사용자 정보 추출
        User user = ((PrincipalDetails) authResult.getPrincipal()).getUser();

        // Access Token 생성
        String accessToken = jwtProvider.createAccessToken(user);

        // Refresh Token 생성
        String refreshToken = jwtProvider.createRefreshToken();

        // Refresh Token을 DB에 저장 (기존 토큰이 있으면 업데이트, 없으면 새로 생성)
        long refreshTokenExpirationMs = jwtProvider.getRefreshTokenExpiration();
        LocalDateTime expiresAt = LocalDateTime.now()
                .plusSeconds(TimeUnit.MILLISECONDS.toSeconds(refreshTokenExpirationMs));

        refreshTokenRepository.findByUserId(user.getUserId())
                .ifPresentOrElse(
                        // 기존 토큰이 있으면 업데이트
                        existingToken -> {
                            existingToken.updateToken(refreshToken, expiresAt);
                            refreshTokenRepository.save(existingToken);
                        },
                        // 없으면 새로 생성
                        () -> {
                            RefreshToken newRefreshToken = RefreshToken.builder()
                                    .token(refreshToken)
                                    .userId(user.getUserId())
                                    .expiresAt(expiresAt)
                                    .build();
                            refreshTokenRepository.save(newRefreshToken);
                        }
                );

        // 쿠키에 토큰 설정
        cookieUtil.createAccessTokenCookie(response, accessToken);
        cookieUtil.createRefreshTokenCookie(response, refreshToken);

        // 응답 데이터 생성 (토큰 제외)
        LoginResponseDto loginResponseDto = LoginResponseDto.fromEntity(user);

        // ResponseDTO로 감싸기
        ResponseDTO<LoginResponseDto> responseDTO = ResponseDTO.okWithData(
                loginResponseDto,
                "로그인에 성공했습니다."
        );

        // JSON 응답 전송
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonResponse = objectMapper.writeValueAsString(responseDTO);

        response.setStatus(HttpStatus.OK.value());
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(jsonResponse);
    }

    /**
     * 인증 실패 시
     */
    @Override
    protected void unsuccessfulAuthentication(
            HttpServletRequest request,
            HttpServletResponse response,
            AuthenticationException exception
    ) throws IOException, ServletException {
        String errorMessage = getAuthenticationErrorMessage(exception);

        // CustomResponseUtil 사용하여 통일된 에러 응답
        CustomResponseUtil.fail(response, errorMessage, HttpStatus.UNAUTHORIZED);
    }

    /**
     * 인증 실패 사유 메시지
     */
    private String getAuthenticationErrorMessage(AuthenticationException exception) {
        if (exception instanceof BadCredentialsException) {
            return "이메일 또는 비밀번호가 일치하지 않습니다.";
        } else if (exception instanceof UsernameNotFoundException) {
            return "존재하지 않는 사용자입니다.";
        } else {
            return "인증에 실패했습니다.";
        }
    }
}
