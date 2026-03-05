package com.exam.stressshop.event;

public interface EventPublisher<T> {
    void publish(T event);
}
