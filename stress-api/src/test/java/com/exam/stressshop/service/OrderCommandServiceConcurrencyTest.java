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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
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

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void multiThreadCrashTest() throws InterruptedException {
        // given
        int userCount = 50;
        int stock = 30;

        Product product = productRepository.save(
                Product.create("상품", BigDecimal.valueOf(1000), (long) stock)
        );

        redisTemplate.opsForValue().set(
                "product:stock:" + product.getId(),
                String.valueOf(stock)
        );

        List<User> users = new ArrayList<>();

        for (int i = 0; i < userCount; i++) {
            User user = userRepository.save(
                    User.create("user" + i,
                            "user" + i + "@test.com",
                            "1234")
            );

            walletRepository.save(
                    Wallet.create(user, BigDecimal.valueOf(100000))
            );

            users.add(user);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(userCount);

        AtomicInteger success = new AtomicInteger();
        AtomicInteger fail = new AtomicInteger();

        // when
        for (User user : users) {
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

        System.out.println("남은 재고: " + updated.getStock());
    }
}