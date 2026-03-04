package com.exam.stressshop.consumer;

import com.exam.stressshop.domain.order.Order;
import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.domain.user.User;
import com.exam.stressshop.event.OrderCreatedEvent;
import com.exam.stressshop.repository.OrderRepository;
import com.exam.stressshop.repository.ProductRepository;
import com.exam.stressshop.repository.UserRepository;
import com.exam.stressshop.repository.WalletRepository;
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

    @KafkaListener(topics = "order-create")
    @Transactional
    public void consume(OrderCreatedEvent event) {

        User user = userRepository.getReferenceById(event.getUserId());
        Product product = productRepository.findById(event.getProductId())
                .orElseThrow();

        BigDecimal totalPrice =
                product.getPrice().multiply(BigDecimal.valueOf(event.getQuantity()));

        // 1️⃣ Wallet 차감
        int walletUpdated =
                walletRepository.decreaseBalance(event.getUserId(), totalPrice);

        if (walletUpdated == 0) {
            throw new RuntimeException("잔액 부족");
        }

        // 2️⃣ DB 재고 최종 차감
        int updated =
                productRepository.decreaseStock(event.getProductId(), event.getQuantity());

        if (updated == 0) {
            throw new RuntimeException("DB 재고 부족");
        }

        // 3️⃣ 주문 저장
        Order order =
                Order.create(user, product, event.getQuantity(), totalPrice);

        orderRepository.save(order);
    }
}