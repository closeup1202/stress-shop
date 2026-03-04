package com.exam.stressshop.domain.order;

import com.exam.stressshop.domain.common.BaseEntity;
import com.exam.stressshop.domain.product.Product;
import com.exam.stressshop.domain.user.User;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(name = "orders")
public class Order extends BaseEntity {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true)
    private String eventId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", nullable = false)
    private User user;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "product_id", nullable = false)
    private Product product;

    @Column(nullable = false)
    private Integer quantity;

    @Column(nullable = false)
    private BigDecimal totalPrice;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private OrderStatus orderStatus;

    public static Order create(String eventId, User user, Product product, int quantity, BigDecimal totalPrice) {
        Order order = new Order();
        order.eventId = eventId;
        order.user = user;
        order.product = product;
        order.quantity = quantity;
        order.totalPrice = totalPrice;
        order.orderStatus = OrderStatus.RECEIVED;
        return order;
    }
}
