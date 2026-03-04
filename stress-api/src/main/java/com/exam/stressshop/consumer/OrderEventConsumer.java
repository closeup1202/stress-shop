package com.exam.stressshop.consumer;

import com.exam.stressshop.domain.order.Order;
import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.domain.user.User;
import com.exam.stressshop.event.OrderCreatedEvent;
import com.exam.stressshop.event.StockRollbackEvent;
import com.exam.stressshop.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final WalletRepository walletRepository;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;
    private final UserRepository userRepository;
    private final EventPublisher<StockRollbackEvent> rollbackPublisher;

    @KafkaListener(topics = "order-create")
    @Transactional
    public void consume(OrderCreatedEvent event) {

        try {
            if (orderRepository.existsByEventId(event.getEventId())) {
                return;
            }

            User user = userRepository.getReferenceById(event.getUserId());
            Product product = productRepository.findById(event.getProductId()).orElseThrow();

            BigDecimal totalPrice =
                    product.getPrice().multiply(BigDecimal.valueOf(event.getQuantity()));

            int walletUpdated =
                    walletRepository.decreaseBalance(event.getUserId(), totalPrice);

            if (walletUpdated == 0) {
                throw new RuntimeException("잔액 부족");
            }

            int updated =
                    productRepository.decreaseStock(event.getProductId(), event.getQuantity());

            if (updated == 0) {
                throw new RuntimeException("DB 재고 부족");
            }

            Order order = Order.create(event.getEventId(), user, product, event.getQuantity(), totalPrice);

            orderRepository.save(order);

        } catch (Exception e) {
            StockRollbackEvent rollbackEvent = StockRollbackEvent.builder()
                    .eventId(event.getEventId())
                    .productId(event.getProductId())
                    .quantity(event.getQuantity())
                    .build();

            rollbackPublisher.publish(rollbackEvent);

            throw e; // Kafka 재시도 유도
        }
    }
}