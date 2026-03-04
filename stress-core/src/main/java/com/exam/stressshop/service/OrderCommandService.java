package com.exam.stressshop.service;

import com.exam.stressshop.domain.order.Order;
import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.domain.user.User;
import com.exam.stressshop.domain.wallet.Wallet;
import com.exam.stressshop.repository.OrderRepository;
import com.exam.stressshop.repository.ProductRepository;
import com.exam.stressshop.repository.UserRepository;
import com.exam.stressshop.repository.WalletRepository;
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

    public Long createOrder(Long userId, Long productId, int quantity) {

        // 1. 사용자 조회
        User user = userRepository.getReferenceById(userId);

        // 2. 상품 조회
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new IllegalArgumentException("상품 없음"));

        // 3. 결제 금액 계산
        BigDecimal totalPrice =
                product.getPrice().multiply(BigDecimal.valueOf(quantity));

        // 4. 지갑 조회 (PK = userId)
        Wallet wallet = walletRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("지갑 없음"));

        // 5. 잔액 차감
        wallet.withdraw(totalPrice);

        // 6. 재고 차감
        product.decreaseStock(quantity);

        // 7. 주문 생성
        Order order = Order.create(user, product, quantity, totalPrice);
        orderRepository.save(order);

        return order.getId();
    }
}
