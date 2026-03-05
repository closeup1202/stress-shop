package com.exam.stressshop.service;

import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.domain.user.User;
import com.exam.stressshop.domain.wallet.Wallet;
import com.exam.stressshop.repository.OrderRepository;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
    private OrderRepository orderRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void step3_multiThread_async_kafka_test() throws InterruptedException {

        // given
        int userCount = 50;
        int stock = 30;
        BigDecimal price = BigDecimal.valueOf(1000);

        Product product = productRepository.save(
                Product.create("상품", price, (long) stock)
        );

        String redisKey = "product:stock:" + product.getId();

        redisTemplate.opsForValue().set(
                redisKey,
                String.valueOf(stock)
        );

        List<User> users = new ArrayList<>();

        for (int i = 0; i < userCount; i++) {
            User user = userRepository.save(
                    User.create("user" + i, "user" + i + "@test.com", "1234")
            );

            walletRepository.save(Wallet.create(user, BigDecimal.valueOf(100000)));
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

        System.out.println("요청 성공(Producer 성공): " + success.get());
        System.out.println("요청 실패(Redis 차감 실패): " + fail.get());

        // 🔥 Consumer 비동기 처리 완료 대기
        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(orderRepository.count()).isEqualTo(stock);
                });

        // then - DB 재고 확인
        Product updatedProduct = productRepository.findById(product.getId()).get();
        System.out.println("DB 남은 재고: " + updatedProduct.getStock());

        // then - Redis 재고 확인
        Long redisStock = Long.valueOf(
                Objects.requireNonNull(redisTemplate.opsForValue().get(redisKey))
        );
        System.out.println("Redis 남은 재고: " + redisStock);

        // then - 주문 개수 확인
        long orderCount = orderRepository.count();
        System.out.println("생성된 주문 수: " + orderCount);

        // then - Wallet 차감 확인
        long totalWalletBalance = walletRepository.findAll()
                .stream()
                .map(Wallet::getBalance)
                .reduce(BigDecimal.ZERO, BigDecimal::add)
                .longValue();

        System.out.println("전체 Wallet 잔액 합계: " + totalWalletBalance);

        // 🔥 검증
        assertThat(success.get()).isEqualTo(stock);
        assertThat(fail.get()).isEqualTo(userCount - stock);

        assertThat(orderCount).isEqualTo(stock);
        assertThat(updatedProduct.getStock()).isEqualTo(0L);
        assertThat(redisStock).isEqualTo(0L);
    }
}