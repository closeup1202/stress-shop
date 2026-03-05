package com.exam.stressshop.api;

import com.exam.stressshop.domain.order.Order;
import com.exam.stressshop.dto.request.OrderCreateRequest;
import com.exam.stressshop.service.OrderCommandService;
import com.exam.stressshop.service.OrderQueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final OrderCommandService orderCommandService;
    private final OrderQueryService orderQueryService;

    @PostMapping
    public void createOrder(@RequestBody OrderCreateRequest request) {
        orderCommandService.createOrder(request.userId(), request.productId(), request.quantity());
        log.info("Order successfully created: {}", request);
    }

    @GetMapping("/{id}")
    public void getOrder(@PathVariable Long id) {
        Order order = orderQueryService.getOrderById(id);
        log.info("Order successfully retrieved: {}", order);
    }
}
