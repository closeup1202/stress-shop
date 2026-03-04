package com.exam.stressshop.publisher;

import com.exam.stressshop.event.StockRollbackEvent;
import com.exam.stressshop.repository.EventPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class StockRollbackPublisher implements EventPublisher<StockRollbackEvent> {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void publish(StockRollbackEvent event) {
        kafkaTemplate.send("stock-rollback", event);
    }
}
