package com.exam.stressshop.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class OrderCreatedEvent {

    private Long userId;
    private Long productId;
    private int quantity;
}