package com.exam.stressshop.repository;

import com.exam.stressshop.domain.order.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
    boolean existsByEventId(String eventId);
}
