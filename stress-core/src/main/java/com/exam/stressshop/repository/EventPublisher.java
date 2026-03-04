package com.exam.stressshop.repository;

public interface EventPublisher<T> {
    void publish(T event);
}
