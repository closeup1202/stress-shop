package com.exam.stressshop.service;

import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.domain.user.User;
import com.exam.stressshop.domain.wallet.Wallet;
import com.exam.stressshop.repository.ProductRepository;
import com.exam.stressshop.repository.UserRepository;
import com.exam.stressshop.repository.WalletRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class OrderCommandServiceConcurrencyTest {

    @Autowired
    private OrderCommandService orderCommandService;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private UserRepository userRepository;

    @Test
    void multiThreadCrashTest() throws InterruptedException {
        // given
        User user = userRepository.save(
                User.create("홍길동", "test@naver.com", "123456")
        );

        Product product = productRepository.save(
                Product.create("상품", BigDecimal.valueOf(1000), 100L)
        );

        Wallet wallet = walletRepository.save(
                Wallet.create(user, BigDecimal.valueOf(100000))
        );

        int threadCount = 50;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        AtomicInteger success = new AtomicInteger();
        AtomicInteger fail = new AtomicInteger();

        // when
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    orderCommandService.createOrder(
                            user.getId(),
                            product.getId(),
                            1
                    );
                    success.incrementAndGet();
                } catch (Exception e) {
                    fail.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();

        // then
        System.out.println("성공: " + success.get());
        System.out.println("실패: " + fail.get());

        Product updated = productRepository.findById(product.getId()).get();
        Wallet updatedWallet = walletRepository.findById(user.getId()).get();

        System.out.println("남은 재고: " + updated.getStock());
        System.out.println("남은 잔액: " + updatedWallet.getBalance());
    }
}