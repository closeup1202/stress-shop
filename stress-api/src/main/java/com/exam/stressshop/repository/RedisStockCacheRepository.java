package com.exam.stressshop.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RedisStockCacheRepository implements StockCacheRepository {

    private final StringRedisTemplate redisTemplate;

    @Override
    public boolean decrease(Long productId, int quantity) {
        String key = "product:stock:" + productId;

        Long stock = redisTemplate.opsForValue().decrement(key, quantity);

        if (stock == null || stock < 0) {
            redisTemplate.opsForValue().increment(key, quantity);
            return false;
        }

        return true;
    }

    @Override
    public void increase(Long productId, int quantity) {
        redisTemplate.opsForValue().increment("product:stock:" + productId, quantity);
    }
}
