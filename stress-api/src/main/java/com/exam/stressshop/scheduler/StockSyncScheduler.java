package com.exam.stressshop.scheduler;

import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class StockSyncScheduler {

    private final ProductRepository productRepository;
    private final StringRedisTemplate redisTemplate;

    @Scheduled(fixedDelay = 30000) // 30초마다
    public void syncStock() {

        List<Product> products = productRepository.findAll();

        for (Product product : products) {

            String key = "product:stock:" + product.getId();

            redisTemplate.opsForValue().setIfAbsent(
                    key,
                    String.valueOf(product.getStock())
            );
        }
    }
}