package com.exam.stressshop.service;

import com.exam.stressshop.domain.order.Order;
import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.domain.user.User;
import com.exam.stressshop.domain.wallet.Wallet;
import com.exam.stressshop.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderCommandService {

    private final OrderRepository orderRepository;
    private final UserRepository userRepository;
    private final ProductRepository productRepository;
    private final WalletRepository walletRepository;
    private final StockCacheRepository stockCacheRepository;

    public Long createOrder(Long userId, Long productId, int quantity) {

        // 1️⃣ Redis 선차감
        boolean redisSuccess = stockCacheRepository.decrease(productId, quantity);

        if (!redisSuccess) {
            throw new IllegalArgumentException("품절");
        }

        try {
            // 2️⃣ 사용자 조회 (프록시 OK)
            User user = userRepository.getReferenceById(userId);

            // 3️⃣ 상품 조회 (가격 계산용)
            Product product = productRepository.findById(productId)
                    .orElseThrow(() -> new IllegalArgumentException("상품 없음"));

            BigDecimal totalPrice =
                    product.getPrice().multiply(BigDecimal.valueOf(quantity));

            // 4️⃣ 지갑 차감 (DB atomic update)
            int walletUpdated =
                    walletRepository.decreaseBalance(userId, totalPrice);

            if (walletUpdated == 0) {
                throw new IllegalArgumentException("잔액 부족");
            }

            // 5️⃣ DB 재고 차감 (정합성 보장용)
            int updated = productRepository.decreaseStock(productId, quantity);

            if (updated == 0) {
                throw new IllegalStateException("DB 재고 부족");
            }

            // 6️⃣ 주문 저장
            Order order = Order.create(user, product, quantity, totalPrice);

            orderRepository.save(order);

            return order.getId();

        } catch (Exception e) {

            // 🔥 실패 시 Redis 롤백
            stockCacheRepository.increase(productId, quantity);

            throw e;
        }
    }
}
