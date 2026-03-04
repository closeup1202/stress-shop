package com.exam.stressshop.repository;

public interface StockCacheRepository {
    boolean decrease(Long productId, int quantity);
    void increase(Long productId, int quantity);
}
