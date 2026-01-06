package com.pawbridge.userservice.config;

import com.pawbridge.userservice.filter.JwtAuthenticationFilter;
import com.pawbridge.userservice.jwt.JwtProvider;
import com.pawbridge.userservice.oauth2.handler.OAuth2FailureHandler;
import com.pawbridge.userservice.oauth2.handler.OAuth2SuccessHandler;
import com.pawbridge.userservice.oauth2.service.CustomOAuth2UserService;
import com.pawbridge.userservice.repository.RefreshTokenRepository;
import com.pawbridge.userservice.util.CookieUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtProvider jwtProvider;
    private final RefreshTokenRepository refreshTokenRepository;
    private final CustomOAuth2UserService customOAuth2UserService;
    private final OAuth2SuccessHandler oAuth2SuccessHandler;
    private final OAuth2FailureHandler oAuth2FailureHandler;
    private final CookieUtil cookieUtil;

    /**
     * BCryptPasswordEncoder Bean 등록
     */
    @Bean
    public BCryptPasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /**
     * Security Filter Chain 설정
     */
    @Bean
    public SecurityFilterChain filterChain(
            HttpSecurity http,
            AuthenticationConfiguration authenticationConfiguration
    ) throws Exception {
        // AuthenticationManager 생성
        AuthenticationManager authenticationManager =
                authenticationConfiguration.getAuthenticationManager();

        // JwtAuthenticationFilter 생성
        JwtAuthenticationFilter jwtAuthenticationFilter =
                new JwtAuthenticationFilter(authenticationManager, jwtProvider, refreshTokenRepository, cookieUtil);

        http
                // CSRF 비활성화 (JWT 사용)
                .csrf(AbstractHttpConfigurer::disable)

                // 세션 사용하지 않음 (Stateless)
                .sessionManagement(session ->
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))

                // 엔드포인트 권한 설정 - > api gateway 에서 검증을 하므로 굳이 더 할 필요가 있나?
                .authorizeHttpRequests(auth -> auth
                        .anyRequest().permitAll()
                )

                // JwtAuthenticationFilter 등록
                .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class)

                // OAuth2 로그인 설정
                .oauth2Login(oauth2 -> oauth2
                        .userInfoEndpoint(userInfo -> userInfo
                                .userService(customOAuth2UserService)
                        )
                        .successHandler(oAuth2SuccessHandler)
                        .failureHandler(oAuth2FailureHandler)
                );

        return http.build();
    }
}
