package com.exam.stressshop.domain.product;

import com.exam.stressshop.domain.common.BaseEntity;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(name = "products")
public class Product extends BaseEntity {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    @Column(nullable = false)
    private BigDecimal price;

    @Column(nullable = false)
    private Long stock;

    @Version
    private Long version;

    public static Product create(String name, BigDecimal price, Long stock) {
        Product product = new Product();
        product.name = name;
        product.price = price;
        product.stock = stock;
        return product;
    }

    public void decreaseStock(long quantity) {
        if (this.stock < quantity) {
            throw new IllegalArgumentException("재고 부족");
        }
        this.stock -= quantity;
    }
}
