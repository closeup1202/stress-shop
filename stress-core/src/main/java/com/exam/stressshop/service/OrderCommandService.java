package com.exam.stressshop.service;

import com.exam.stressshop.event.OrderCreatedEvent;
import com.exam.stressshop.repository.EventPublisher;
import com.exam.stressshop.repository.StockCacheRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderCommandService {

    private final StockCacheRepository stockCacheRepository;
    private final EventPublisher<OrderCreatedEvent> eventPublisher;

    public void createOrder(Long userId, Long productId, int quantity) {

        if (!stockCacheRepository.decrease(productId, quantity)) {
            throw new IllegalArgumentException("품절");
        }

        String eventId = UUID.randomUUID().toString();

        try {
            OrderCreatedEvent orderCreatedEvent = OrderCreatedEvent.builder()
                    .eventId(eventId)
                    .userId(userId)
                    .productId(productId)
                    .quantity(quantity)
                    .build();

            eventPublisher.publish(orderCreatedEvent);
        } catch (Exception e) {
            stockCacheRepository.increase(productId, quantity);
            throw e;
        }
    }
}
