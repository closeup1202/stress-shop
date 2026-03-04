package com.exam.stressshop.service;

import com.exam.stressshop.event.OrderCreatedEvent;
import com.exam.stressshop.repository.EventPublisher;
import com.exam.stressshop.repository.StockCacheRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderCommandService {

    private final StockCacheRepository stockCacheRepository;
    private final EventPublisher<OrderCreatedEvent> eventPublisher;

    public void createOrder(Long userId, Long productId, int quantity) {

        // 1️⃣ Redis 선차감
        if (!stockCacheRepository.decrease(productId, quantity)) {
            throw new IllegalArgumentException("품절");
        }

        try {
            eventPublisher.publish(
                    new OrderCreatedEvent(userId, productId, quantity)
            );
        } catch (Exception e) {
            stockCacheRepository.increase(productId, quantity);
            throw e;
        }
    }
}
