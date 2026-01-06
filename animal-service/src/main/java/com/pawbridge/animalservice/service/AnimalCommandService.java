package com.pawbridge.animalservice.service;

import com.pawbridge.animalservice.dto.request.CreateAnimalRequest;
import com.pawbridge.animalservice.dto.request.UpdateAnimalDescriptionRequest;
import com.pawbridge.animalservice.dto.request.UpdateAnimalStatusRequest;
import com.pawbridge.animalservice.dto.response.AnimalDetailResponse;
import com.pawbridge.animalservice.entity.Animal;
import com.pawbridge.animalservice.entity.Shelter;
import com.pawbridge.animalservice.enums.AnimalStatus;
import com.pawbridge.animalservice.enums.ApiSource;
import com.pawbridge.animalservice.event.AnimalCreatedEvent;
import com.pawbridge.animalservice.event.AnimalStatusChangedEvent;
import com.pawbridge.animalservice.event.AnimalUpdatedEvent;
import com.pawbridge.animalservice.mapper.AnimalMapper;
import com.pawbridge.animalservice.repository.AnimalRepository;
import com.pawbridge.animalservice.repository.ShelterRepository;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Animal Command Service (CUD)
 * - 동물 생성, 수정, 삭제 담당
 * - 쓰기 작업 트랜잭션 관리
 * - DTO 기반 처리
 */
@Service
@RequiredArgsConstructor
public class AnimalCommandService {

    // Kafka 토픽 상수
    private static final String TOPIC_ANIMAL_CREATED = "animal.created";
    private static final String TOPIC_ANIMAL_UPDATED = "animal.updated";
    private static final String TOPIC_ANIMAL_STATUS_CHANGED = "animal.status.changed";

    private final AnimalRepository animalRepository;
    private final ShelterRepository shelterRepository;
    private final AnimalMapper mapper;
    private final OutboxService outboxService;
    private final NoticeNumberGenerator noticeNumberGenerator;

    /**
     * 동물 생성 (보호소 직접 등록)
     * @param request 동물 등록 요청 DTO
     * @return AnimalDetailResponse
     * @throws EntityNotFoundException Shelter가 없을 때
     */
    @Transactional
    public AnimalDetailResponse create(CreateAnimalRequest request) {
        // Shelter 조회 (careRegNo로)
        Shelter shelter = shelterRepository.findByCareRegNo(request.getCareRegNo())
                .orElseThrow(() -> new EntityNotFoundException("Shelter not found: " + request.getCareRegNo()));

        // apiSource 기본값 처리 (미입력 시 MANUAL)
        if (request.getApiSource() == null) {
            request.setApiSource(ApiSource.MANUAL);
        }

        // 수동 등록 시 공고번호 자동 생성
        if (request.getApiSource() == ApiSource.MANUAL && 
            (request.getApmsNoticeNo() == null || request.getApmsNoticeNo().isBlank())) {
            request.setApmsNoticeNo(noticeNumberGenerator.generate());
        }

        // DTO → Entity 변환
        Animal animal = mapper.toEntity(request, shelter);

        // 저장
        Animal saved = animalRepository.save(animal);

        // Outbox에 이벤트 저장 (같은 트랜잭션)
        AnimalCreatedEvent event = AnimalCreatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ANIMAL_CREATED")
                .timestamp(LocalDateTime.now())
                .animalId(saved.getId())
                .species(saved.getSpecies().name())
                .breed(saved.getBreed())
                .status(saved.getStatus().name())
                .shelterId(saved.getShelter().getId())
                .build();
        outboxService.saveEvent("Animal", String.valueOf(saved.getId()),
                "ANIMAL_CREATED", TOPIC_ANIMAL_CREATED, event);

        // Entity → Response DTO
        return mapper.toDetailResponse(saved);
    }

    /**
     * 동물 상태 변경
     * @param id 동물 ID
     * @param request 상태 변경 요청 DTO
     * @return AnimalDetailResponse
     * @throws EntityNotFoundException 동물이 없을 때
     */
    @Transactional
    public AnimalDetailResponse updateStatus(Long id, UpdateAnimalStatusRequest request) {
        Animal animal = animalRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("Animal not found: " + id));

        AnimalStatus oldStatus = animal.getStatus();
        animal.updateStatus(request.getNewStatus());

        // Outbox에 이벤트 저장 (같은 트랜잭션)
        AnimalStatusChangedEvent event = AnimalStatusChangedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ANIMAL_STATUS_CHANGED")
                .timestamp(LocalDateTime.now())
                .animalId(animal.getId())
                .oldStatus(oldStatus.name())
                .newStatus(animal.getStatus().name())
                .animalName(animal.getApmsNoticeNo())
                .species(animal.getSpecies().name())
                .build();
        outboxService.saveEvent("Animal", String.valueOf(animal.getId()),
                "ANIMAL_STATUS_CHANGED", TOPIC_ANIMAL_STATUS_CHANGED, event);

        return mapper.toDetailResponse(animal);
    }

    /**
     * 보호소 설명 수정
     * @param id 동물 ID
     * @param request 설명 수정 요청 DTO
     * @return AnimalDetailResponse
     * @throws EntityNotFoundException 동물이 없을 때
     */
    @Transactional
    public AnimalDetailResponse updateDescription(Long id, UpdateAnimalDescriptionRequest request) {
        Animal animal = animalRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("Animal not found: " + id));

        animal.updateDescription(request.getDescription());

        // Outbox에 이벤트 저장 (같은 트랜잭션)
        Map<String, Object> updatedFields = new HashMap<>();
        updatedFields.put("description", request.getDescription());

        AnimalUpdatedEvent event = AnimalUpdatedEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ANIMAL_UPDATED")
                .timestamp(LocalDateTime.now())
                .animalId(animal.getId())
                .updatedFields(updatedFields)
                .build();
        outboxService.saveEvent("Animal", String.valueOf(animal.getId()),
                "ANIMAL_UPDATED", TOPIC_ANIMAL_UPDATED, event);

        return mapper.toDetailResponse(animal);
    }

    /**
     * 찜 횟수 증가
     * - user-service에서 Kafka 이벤트 받아 호출
     * @param id 동물 ID
     * @throws EntityNotFoundException 동물이 없을 때
     */
    @Transactional
    public void incrementFavoriteCount(Long id) {
        Animal animal = animalRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("Animal not found: " + id));

        animal.incrementFavoriteCount();
    }

    /**
     * 찜 횟수 감소
     * @param id 동물 ID
     * @throws EntityNotFoundException 동물이 없을 때
     */
    @Transactional
    public void decrementFavoriteCount(Long id) {
        Animal animal = animalRepository.findById(id)
                .orElseThrow(() -> new EntityNotFoundException("Animal not found: " + id));

        animal.decrementFavoriteCount();
    }

    /**
     * 동물 삭제
     * @param id 동물 ID
     * @throws EntityNotFoundException 동물이 없을 때
     */
    @Transactional
    public void delete(Long id) {
        if (!animalRepository.existsById(id)) {
            throw new EntityNotFoundException("Animal not found: " + id);
        }
        animalRepository.deleteById(id);
    }

    /**
     * APMS 데이터로부터 동물 생성 (배치 전용)
     * - 중복 체크 포함
     * - ApmsDataMapper에서 Entity를 만들어서 전달
     * @param animal 동물 Entity
     * @return 저장된 Animal
     * @throws IllegalStateException 이미 존재하는 동물일 때
     */
    @Transactional
    public Animal createFromApms(Animal animal) {
        // 중복 체크
        if (animal.getApmsDesertionNo() != null &&
                animalRepository.existsByApmsDesertionNo(animal.getApmsDesertionNo())) {
            throw new IllegalStateException("Animal already exists: " + animal.getApmsDesertionNo());
        }

        return animalRepository.save(animal);
    }

    /**
     * 공고 종료된 동물 상태 일괄 업데이트
     * - 배치 작업용: 공고 종료일이 지난 보호 중인 동물 처리
     * - 현재는 PROTECT 상태 유지 (향후 다른 로직 추가 가능)
     * @return 업데이트된 개수
     */
    @Transactional
    public int updateExpiredProtectAnimals() {
        List<Animal> expiredAnimals = animalRepository.findExpiredProtectAnimals(LocalDate.now());

        // 향후 필요 시 상태 변경 로직 추가
        // 예: expiredAnimals.forEach(animal -> animal.updateStatus(AnimalStatus.UNKNOWN));

        return expiredAnimals.size();
    }
}
