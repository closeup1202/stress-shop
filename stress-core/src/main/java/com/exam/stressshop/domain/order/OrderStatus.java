package com.exam.stressshop.domain.order;

public enum OrderStatus {
    PENDING,       // 주문 접수, Consumer 처리 대기
    COMPLETED,     // Consumer 처리 성공
    FAILED,        // 재시도 모두 소진, DLQ 도달
    SHIPPED,
    CANCELED,
}
