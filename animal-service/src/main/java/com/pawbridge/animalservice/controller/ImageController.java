package com.pawbridge.animalservice.controller;

import com.pawbridge.animalservice.service.S3Service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 동물 이미지 업로드 API 컨트롤러
 * - 단일/다중 이미지 업로드 지원
 * - S3에 저장 후 URL 반환
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/animals/images")
@RequiredArgsConstructor
public class ImageController {

    private final S3Service s3Service;

    /**
     * 단일 이미지 업로드
     * POST /api/v1/animals/images
     *
     * @param file 업로드할 이미지 파일
     * @return 업로드된 이미지 URL
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> uploadImage(
            @RequestParam("file") MultipartFile file) {

        log.info("이미지 업로드 요청: filename={}, size={}", 
                file.getOriginalFilename(), file.getSize());

        String imageUrl = s3Service.uploadImage(file);

        Map<String, String> response = new HashMap<>();
        response.put("imageUrl", imageUrl);

        return ResponseEntity.ok(response);
    }

    /**
     * 다중 이미지 업로드
     * POST /api/v1/animals/images/multiple
     *
     * @param files 업로드할 이미지 파일들 (최대 5개)
     * @return 업로드된 이미지 URL 목록
     */
    @PostMapping("/multiple")
    public ResponseEntity<Map<String, Object>> uploadMultipleImages(
            @RequestParam("files") MultipartFile[] files) {

        if (files == null || files.length == 0) {
            throw new IllegalArgumentException("업로드할 파일이 없습니다.");
        }

        if (files.length > 5) {
            throw new IllegalArgumentException("최대 5개의 이미지만 업로드할 수 있습니다.");
        }

        log.info("다중 이미지 업로드 요청: count={}", files.length);

        List<String> imageUrls = new ArrayList<>();
        for (MultipartFile file : files) {
            String imageUrl = s3Service.uploadImage(file);
            imageUrls.add(imageUrl);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("imageUrls", imageUrls);
        response.put("count", imageUrls.size());

        return ResponseEntity.ok(response);
    }

    /**
     * 이미지 삭제
     * DELETE /api/v1/animals/images
     *
     * @param imageUrl 삭제할 이미지 URL
     * @return 성공 메시지
     */
    @DeleteMapping
    public ResponseEntity<Map<String, String>> deleteImage(
            @RequestParam("imageUrl") String imageUrl) {

        log.info("이미지 삭제 요청: url={}", imageUrl);

        s3Service.deleteImage(imageUrl);

        Map<String, String> response = new HashMap<>();
        response.put("message", "이미지가 삭제되었습니다.");

        return ResponseEntity.ok(response);
    }
}
