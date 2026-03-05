package com.exam.stressshop.dto.request;

public record OrderCreateRequest(
        Long userId, Long productId, int quantity) {
}
