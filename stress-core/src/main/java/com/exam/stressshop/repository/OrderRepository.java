package com.exam.stressshop.repository;

import com.exam.stressshop.domain.order.Order;
import com.exam.stressshop.domain.order.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface OrderRepository extends JpaRepository<Order, Long> {
    boolean existsByEventId(String eventId);
    Optional<Order> findByEventId(String eventId);
    long countByOrderStatus(OrderStatus orderStatus);
}
