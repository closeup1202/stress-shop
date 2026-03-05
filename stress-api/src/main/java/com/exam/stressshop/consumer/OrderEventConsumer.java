package com.exam.stressshop.consumer;

import com.exam.stressshop.domain.order.Order;
import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.domain.user.User;
import com.exam.stressshop.event.EventPublisher;
import com.exam.stressshop.event.OrderCreatedEvent;
import com.exam.stressshop.event.StockRollbackEvent;
import com.exam.stressshop.repository.OrderRepository;
import com.exam.stressshop.repository.ProductRepository;
import com.exam.stressshop.repository.UserRepository;
import com.exam.stressshop.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {

    private final WalletRepository walletRepository;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;
    private final UserRepository userRepository;
    private final EventPublisher<StockRollbackEvent> rollbackPublisher;

    @KafkaListener(
            topics = "order-create",
            groupId = "order-group"
    )
    @Transactional
    public void consume(OrderCreatedEvent event) {
        log.info("OrderCreatedEvent: {}", event);
        if (orderRepository.existsByEventId(event.getEventId())) {
            return;
        }

        User user = userRepository.getReferenceById(event.getUserId());
        Product product = productRepository.findById(event.getProductId()).orElseThrow();

        BigDecimal totalPrice = product.getPrice().multiply(BigDecimal.valueOf(event.getQuantity()));

        int walletUpdated = walletRepository.decreaseBalance(event.getUserId(), totalPrice);

        if (walletUpdated == 0) {
            throw new RuntimeException("잔액 부족");
        }

        int updated = productRepository.decreaseStock(event.getProductId(), event.getQuantity());

        if (updated == 0) {
            throw new RuntimeException("DB 재고 부족");
        }

        Order order = Order.create(event.getEventId(), user, product, event.getQuantity(), totalPrice);

        orderRepository.save(order);
    }

    // 재시도 모두 소진 후 DLQ 도달 시에만 재고 롤백
    @KafkaListener(topics = "order-create.DLQ", groupId = "order-group")
    public void handleDlq(OrderCreatedEvent event) {
        log.error("DLQ 도착 이벤트: {}", event);

        StockRollbackEvent rollbackEvent = StockRollbackEvent.builder()
                .eventId(event.getEventId())
                .productId(event.getProductId())
                .quantity(event.getQuantity())
                .build();

        rollbackPublisher.publish(rollbackEvent);
    }
}
