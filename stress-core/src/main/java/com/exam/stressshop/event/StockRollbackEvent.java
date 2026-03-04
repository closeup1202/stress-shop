package com.exam.stressshop.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class StockRollbackEvent {

    private String eventId;
    private Long productId;
    private int quantity;
}
