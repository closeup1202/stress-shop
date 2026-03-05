package com.exam.stressshop.scheduler;

import com.exam.stressshop.domain.outbox.OutboxEvent;
import com.exam.stressshop.event.OrderCreatedEvent;
import com.exam.stressshop.repository.OutboxEventRepository;
import com.exam.stressshop.service.EventPoller;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller implements EventPoller {

    private final OutboxEventRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 1000)  // 1초마다 폴링
    @Transactional
    @Override
    public void poll() {
        List<OutboxEvent> events = outboxRepository.findPendingWithLock(
                PageRequest.of(0, 100)  // 한 번에 최대 100건
        );

        for (OutboxEvent event : events) {
            try {
                // payload에서 userId 추출 (파티션 키 유지)
                OrderCreatedEvent orderEvent = objectMapper.readValue(event.getPayload(), OrderCreatedEvent.class);

                kafkaTemplate.send(event.getTopic(), orderEvent.getUserId().toString(), orderEvent).get();

                event.markPublished();

            } catch (Exception e) {
                log.error("Outbox 발행 실패 - eventId: {}", event.getId(), e);
                // 실패 시 status 변경 없음 → 다음 폴링 사이클에서 재시도
            }
        }
    }
}
