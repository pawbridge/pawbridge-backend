package com.pawbridge.animalservice.service;

import com.pawbridge.animalservice.repository.AnimalRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * 수동 등록 동물의 공고번호 생성기
 * - 형식: MAN-{YYMMDD}-{6자리순번}
 * - 예: MAN-251230-000001
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NoticeNumberGenerator {

    private static final String PREFIX = "MAN";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyMMdd");

    private final AnimalRepository animalRepository;

    /**
     * 수동 등록용 공고번호 생성
     * @return 고유한 공고번호
     */
    public String generate() {
        String today = LocalDate.now().format(DATE_FORMAT);
        String prefix = PREFIX + "-" + today + "-";

        // 오늘 날짜의 마지막 순번 조회
        long count = animalRepository.countByApmsNoticeNoStartingWith(prefix);
        long nextNumber = count + 1;

        // 6자리 순번 (000001 ~ 999999)
        String noticeNo = prefix + String.format("%06d", nextNumber);

        log.info("수동 등록 공고번호 생성: {}", noticeNo);
        return noticeNo;
    }
}
