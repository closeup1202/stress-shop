package com.exam.stressshop.consumer;

import com.exam.stressshop.event.StockRollbackEvent;
import com.exam.stressshop.repository.StockCacheRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class StockRollbackConsumer {

    private final StockCacheRepository stockCacheRepository;

    @KafkaListener(topics = "stock-rollback")
    public void rollback(StockRollbackEvent event) {
        stockCacheRepository.increase(
                event.getProductId(),
                event.getQuantity()
        );
    }
}