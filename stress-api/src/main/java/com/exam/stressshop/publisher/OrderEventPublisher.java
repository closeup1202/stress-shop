package com.exam.stressshop.publisher;

import com.exam.stressshop.event.OrderCreatedEvent;
import com.exam.stressshop.event.EventPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderEventPublisher implements EventPublisher<OrderCreatedEvent> {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void publish(OrderCreatedEvent event) {
        try {
            kafkaTemplate.send("order-create", event.getUserId().toString(), event).get();
        } catch (Exception e) {
            throw new RuntimeException("Kafka 발행 실패", e);
        }
    }
}