package com.exam.stressshop.consumer;

import com.exam.stressshop.domain.stockrollback.StockRollbackHistory;
import com.exam.stressshop.event.StockRollbackEvent;
import com.exam.stressshop.repository.StockCacheRepository;
import com.exam.stressshop.repository.StockRollbackRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class StockRollbackConsumer {

    private final StockCacheRepository stockCacheRepository;
    private final StockRollbackRepository historyRepository;

    @KafkaListener(
            topics = "stock-rollback",
            groupId = "stock-rollback-group",
            concurrency = "1"
    )
    @Transactional
    public void rollback(StockRollbackEvent event) {
        // 이미 복구했으면 종료
        if (historyRepository.existsById(event.getEventId())) {
            return;
        }

        stockCacheRepository.increase(event.getProductId(), event.getQuantity());
        historyRepository.save(new StockRollbackHistory(event.getEventId()));
    }
}