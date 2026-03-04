package com.exam.stressshop.service;

import com.exam.stressshop.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderQueryService {

    private final OrderRepository orderRepository;

    public void getOrdersByUserId() {

    }
}
