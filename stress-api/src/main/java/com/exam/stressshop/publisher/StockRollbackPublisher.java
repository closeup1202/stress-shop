package com.exam.stressshop.publisher;

import com.exam.stressshop.event.StockRollbackEvent;
import com.exam.stressshop.event.EventPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class StockRollbackPublisher implements EventPublisher<StockRollbackEvent> {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void publish(StockRollbackEvent event) {
        try {
            kafkaTemplate.send("stock-rollback", event).get();
        } catch (Exception e) {
            throw new RuntimeException("재고 롤백 이벤트 발행 실패", e);
        }
    }
}
