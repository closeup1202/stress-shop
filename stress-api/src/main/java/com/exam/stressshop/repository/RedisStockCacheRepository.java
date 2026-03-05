package com.exam.stressshop.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisStockCacheRepository implements StockCacheRepository {

    private final StringRedisTemplate redisTemplate;

    @Override
    public boolean decrease(Long productId, int quantity) {
        String key = "product:stock:" + productId;

        Long result = redisTemplate.execute(
                DECREASE_SCRIPT,
                List.of(key),
                String.valueOf(quantity)
        );

        return result != null && result >= 0;
    }

    @Override
    public void increase(Long productId, int quantity) {
        redisTemplate.opsForValue().increment("product:stock:" + productId, quantity);
    }

    private static final RedisScript<Long> DECREASE_SCRIPT = RedisScript.of(
            """
            local stock = tonumber(redis.call('GET', KEYS[1]))
            if stock == nil then
                return -2
            end
            if stock < tonumber(ARGV[1]) then
                return -1
            end
            return redis.call('DECRBY', KEYS[1], ARGV[1])
            """,
            Long.class
    );
}
