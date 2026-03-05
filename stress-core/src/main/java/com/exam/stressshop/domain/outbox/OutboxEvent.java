package com.exam.stressshop.domain.outbox;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Getter
@Table(name = "outbox_events")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEvent {

    @Id
    private String id;           // = eventId (Order와 동일한 UUID)

    @Column(nullable = false)
    private String topic;        // "order-create"

    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;      // JSON 직렬화된 이벤트

    @Enumerated(EnumType.STRING)
    private OutboxStatus status; // PENDING → PUBLISHED

    private LocalDateTime createdAt;
    private LocalDateTime publishedAt;

    public static OutboxEvent create(String id, String topic, String payload) {
        OutboxEvent event = new OutboxEvent();
        event.id = id;
        event.topic = topic;
        event.payload = payload;
        event.status = OutboxStatus.PENDING;
        event.createdAt = LocalDateTime.now();
        return event;
    }

    public void markPublished() {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = LocalDateTime.now();
    }
}
