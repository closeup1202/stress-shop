package com.exam.stressshop.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class OrderMetrics {

    private final Counter successCounter;
    private final Counter failedCounter;
    private final Counter dlqCounter;

    public OrderMetrics(MeterRegistry registry) {
        this.successCounter = Counter.builder("order_success_total")
                .description("주문 처리 성공 수")
                .register(registry);

        this.failedCounter = Counter.builder("order_failed_total")
                .description("주문 처리 실패 수 (재시도 포함)")
                .register(registry);

        this.dlqCounter = Counter.builder("order_dlq_total")
                .description("DLQ 도달 주문 수")
                .register(registry);
    }

    public void incrementSuccess() { successCounter.increment(); }
    public void incrementFailed() { failedCounter.increment(); }
    public void incrementDlq() { dlqCounter.increment(); }
}
