package com.pawbridge.animalservice.dto.request;

import com.pawbridge.animalservice.enums.AnimalStatus;
import com.pawbridge.animalservice.enums.ApiSource;
import com.pawbridge.animalservice.enums.Gender;
import com.pawbridge.animalservice.enums.NeuterStatus;
import com.pawbridge.animalservice.enums.Species;
import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

/**
 * 동물 등록 요청 DTO
 * - 보호소 회원/관리자가 동물을 수동 등록할 때 사용
 * - APMS 배치는 별도 DTO(ApmsAnimalDto) 사용
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateAnimalRequest {

    // 필수 필드 (9개)
    /**
     * 보호소 등록번호
     * - 예: "411310000001"
     * - /api/users/me에서 careRegNo 획득
     */
    @NotBlank(message = "보호소 등록번호는 필수입니다")
    @Size(max = 50, message = "보호소 등록번호는 50자 이하여야 합니다")
    private String careRegNo;

    /**
     * 공고번호 (선택)
     * - APMS 데이터: 직접 입력 (예: "경기-양평-2025-00429")
     * - 수동 등록: 미입력 시 자동 생성 (예: "MAN-251230-000001")
     */
    @Size(max = 100, message = "공고번호는 100자 이하여야 합니다")
    private String apmsNoticeNo;

    /**
     * 공고 시작일
     */
    @NotNull(message = "공고 시작일은 필수입니다")
    private LocalDate noticeStartDate;

    /**
     * 공고 종료일
     */
    @NotNull(message = "공고 종료일은 필수입니다")
    private LocalDate noticeEndDate;

    /**
     * 축종 (개/고양이/기타)
     */
    @NotNull(message = "축종은 필수입니다")
    private Species species;

    /**
     * 성별 (수컷/암컷/미상)
     */
    @NotNull(message = "성별은 필수입니다")
    private Gender gender;

    /**
     * 중성화 여부 (예/아니오/미상)
     */
    @NotNull(message = "중성화 여부는 필수입니다")
    private NeuterStatus neuterStatus;

    /**
     * 동물 상태
     * - NOTICE(공고중), PROTECT(보호중), ADOPTION_PENDING(입양대기), ADOPTED(입양완료) 등
     */
    @NotNull(message = "동물 상태는 필수입니다")
    private AnimalStatus status;

    /**
     * 데이터 출처 (선택, 기본값: MANUAL)
     * - MANUAL(수동등록), APMS_ANIMAL(APMS API) 등
     * - 미입력 시 MANUAL로 처리
     */
    private ApiSource apiSource;

    // 선택 필드 (9개)
    /**
     * 품종명
     * - 예: "믹스견", "라브라도 리트리버"
     */
    @Size(max = 100, message = "품종명은 100자 이하여야 합니다")
    private String breed;

    /**
     * 출생 연도
     * - 예: 2023
     */
    @Min(value = 1900, message = "출생연도는 1900년 이후여야 합니다")
    @Max(value = 2100, message = "출생연도는 2100년 이하여야 합니다")
    private Integer birthYear;

    /**
     * 체중
     * - 예: "12(Kg)"
     */
    @Size(max = 50, message = "체중은 50자 이하여야 합니다")
    private String weight;

    /**
     * 색상
     * - 예: "옅은 황색", "검정&흰색"
     */
    @Size(max = 100, message = "색상은 100자 이하여야 합니다")
    private String color;

    /**
     * 특징
     * - 예: "엄청 까불까불하고 순해요~잘생김~"
     */
    @Size(max = 1000, message = "특징은 1000자 이하여야 합니다")
    private String specialMark;

    /**
     * 발견 장소
     * - 예: "진해구 남문동 남문시티 1차"
     */
    @Size(max = 200, message = "발견장소는 200자 이하여야 합니다")
    private String happenPlace;

    /**
     * 대표 이미지 URL
     */
    @Size(max = 500, message = "이미지 URL은 500자 이하여야 합니다")
    private String imageUrl;

    /**
     * 추가 이미지 URL
     */
    @Size(max = 500, message = "이미지 URL은 500자 이하여야 합니다")
    private String imageUrl2;

    /**
     * 보호소가 추가 작성한 설명
     * - APMS 데이터 외 보호소가 직접 입력하는 상세 설명
     */
    @Size(max = 2000, message = "설명은 2000자 이하여야 합니다")
    private String description;
}
