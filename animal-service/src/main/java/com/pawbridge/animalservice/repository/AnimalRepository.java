package com.pawbridge.animalservice.repository;

import com.pawbridge.animalservice.entity.Animal;
import com.pawbridge.animalservice.enums.AnimalStatus;
import com.pawbridge.animalservice.enums.Gender;
import com.pawbridge.animalservice.enums.Species;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Animal 엔티티 리포지토리
 * - 동물 정보 CRUD 및 검색 쿼리
 * - N+1 문제 방지: 단건 상세는 @EntityGraph, 목록은 DTO 프로젝션 사용
 * - JpaSpecificationExecutor: 동적 검색 쿼리 지원 (Phase 1 - OpenSearch 전환 전까지)
 */
@Repository
public interface AnimalRepository extends JpaRepository<Animal, Long>,
                                          org.springframework.data.jpa.repository.JpaSpecificationExecutor<Animal> {

    //기본 조회 (UNIQUE KEY)
    /**
     * APMS 유기번호로 조회 (UNIQUE)
     * - 배치 작업에서 중복 체크용
     * @param apmsDesertionNo APMS 유기번호
     * @return Animal Optional
     */
    Optional<Animal> findByApmsDesertionNo(String apmsDesertionNo);

    /**
     * APMS 유기번호 존재 여부 확인
     * @param apmsDesertionNo APMS 유기번호
     * @return 존재 여부
     */
    boolean existsByApmsDesertionNo(String apmsDesertionNo);

    //단건 상세 조회 (Shelter 정보 포함)
    /**
     * ID로 동물 조회 (Shelter fetch join)
     * - 상세 페이지용
     * - @EntityGraph로 N+1 방지
     * @param id 동물 ID
     * @return Animal Optional with Shelter
     */
    @EntityGraph(attributePaths = {"shelter"})
    Optional<Animal> findWithShelterById(Long id);

    /**
     * APMS 유기번호로 조회 (Shelter fetch join)
     * @param apmsDesertionNo APMS 유기번호
     * @return Animal Optional with Shelter
     */
    @EntityGraph(attributePaths = {"shelter"})
    Optional<Animal> findWithShelterByApmsDesertionNo(String apmsDesertionNo);


    // 보호소별 조회

    /**
     * 보호소 ID로 동물 목록 조회
     * @param shelterId 보호소 ID
     * @param pageable 페이징 정보
     * @return Page<Animal>
     */
    Page<Animal> findByShelterId(Long shelterId, Pageable pageable);

    /**
     * 보호소 ID + 축종으로 조회
     * @param shelterId 보호소 ID
     * @param species 축종
     * @param pageable 페이징 정보
     * @return Page<Animal>
     */
    Page<Animal> findByShelterIdAndSpecies(Long shelterId, Species species, Pageable pageable);

    /**
     * 보호소 ID + 상태로 조회
     * @param shelterId 보호소 ID
     * @param status 상태
     * @param pageable 페이징 정보
     * @return Page<Animal>
     */
    Page<Animal> findByShelterIdAndStatus(Long shelterId, AnimalStatus status, Pageable pageable);

    // 공고 관련 조회

    /**
     * 공고 종료 임박 동물 조회 (D-3 이내)
     * @param today 오늘 날짜
     * @param deadline 임박 기준일 (오늘 + 3일)
     * @param pageable 페이징 정보
     * @return Page<Animal>
     */
    @Query("SELECT a FROM Animal a WHERE a.status = 'PROTECT' AND a.noticeEndDate BETWEEN :today AND :deadline ORDER BY a.noticeEndDate ASC")
    Page<Animal> findExpiringSoonAnimals(
            @Param("today") LocalDate today,
            @Param("deadline") LocalDate deadline,
            Pageable pageable
    );


    /**
     * 축종별 동물 수 카운트
     * @param species 축종
     * @return 개수
     */
    long countBySpecies(Species species);

    /**
     * 상태별 동물 수 카운트
     * @param status 상태
     * @return 개수
     */
    long countByStatus(AnimalStatus status);

    /**
     * 보호소별 동물 수 카운트
     * @param shelterId 보호소 ID
     * @return 개수
     */
    long countByShelterId(Long shelterId);

    /**
     * 축종 + 상태별 카운트
     * @param species 축종
     * @param status 상태
     * @return 개수
     */
    long countBySpeciesAndStatus(Species species, AnimalStatus status);

    /**
     * 특정 날짜 이후 APMS에서 수정된 동물 조회
     * - APMS 동기화 시 증분 업데이트용
     * @param dateTime 기준 날짜시간
     * @return 동물 목록
     */
    @Query("SELECT a FROM Animal a WHERE a.apmsUpdatedAt > :dateTime")
    List<Animal> findByApmsUpdatedAtAfter(@Param("dateTime") LocalDateTime dateTime);

    /**
     * 공고 종료일이 지난 동물 조회
     * - 상태 자동 업데이트 배치용
     * @param today 오늘 날짜
     * @return 동물 목록
     */
    @Query("SELECT a FROM Animal a WHERE a.status = 'PROTECT' AND a.noticeEndDate < :today")
    List<Animal> findExpiredProtectAnimals(@Param("today") LocalDate today);

    /**
     * 특정 prefix로 시작하는 공고번호 개수 조회
     * - 수동 등록 순번 생성용
     * @param prefix 공고번호 prefix (예: "MAN-251230-")
     * @return 개수
     */
    long countByApmsNoticeNoStartingWith(String prefix);
}
