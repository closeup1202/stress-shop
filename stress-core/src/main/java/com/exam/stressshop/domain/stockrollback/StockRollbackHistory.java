package com.exam.stressshop.domain.stockrollback;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Getter
@Table(name = "stock_rollback_history")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class StockRollbackHistory {

    @Id
    private String eventId;

    private LocalDateTime createdAt;

    public StockRollbackHistory(String eventId) {
        this.eventId = eventId;
        this.createdAt = LocalDateTime.now();
    }
}
