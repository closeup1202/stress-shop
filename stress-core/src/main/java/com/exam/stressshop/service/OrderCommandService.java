package com.exam.stressshop.service;

import com.exam.stressshop.domain.order.Order;
import com.exam.stressshop.domain.outbox.OutboxEvent;
import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.domain.user.User;
import com.exam.stressshop.event.OrderCreatedEvent;
import com.exam.stressshop.repository.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderCommandService {

    private final StockCacheRepository stockCacheRepository;
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final UserRepository userRepository;
    private final ProductRepository productRepository;
    private final ObjectMapper objectMapper;


    public void createOrder(Long userId, Long productId, int quantity) {

        // 1. Redis 선차감
        if (!stockCacheRepository.decrease(productId, quantity)) {
            throw new IllegalArgumentException("품절");
        }

        try {
            User user = userRepository.getReferenceById(userId);
            Product product = productRepository.findById(productId).orElseThrow();
            BigDecimal totalPrice = product.getPrice().multiply(BigDecimal.valueOf(quantity));

            String eventId = UUID.randomUUID().toString();

            // 2. Order 저장 (PENDING) - DB 트랜잭션 내
            Order order = Order.create(eventId, user, product, quantity, totalPrice);
            orderRepository.save(order);

            // 3. Outbox 이벤트 저장 - 같은 DB 트랜잭션 내 (핵심!)
            OrderCreatedEvent event = OrderCreatedEvent.builder()
                    .eventId(eventId)
                    .userId(userId)
                    .productId(productId)
                    .quantity(quantity)
                    .build();

            try {
                String payload = objectMapper.writeValueAsString(event);
                outboxRepository.save(OutboxEvent.create(eventId, "order-create", payload));
            } catch (JsonProcessingException e) {
                throw new RuntimeException("이벤트 직렬화 실패", e);
            }

        } catch (Exception e) {
            // DB 실패 시 Redis 수동 복구
            stockCacheRepository.increase(productId, quantity);
            throw e;
        }
    }
}
