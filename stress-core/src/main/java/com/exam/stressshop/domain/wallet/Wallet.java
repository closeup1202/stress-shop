package com.exam.stressshop.domain.wallet;

import com.exam.stressshop.domain.common.BaseEntity;
import com.exam.stressshop.domain.user.User;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(name = "wallets")
public class Wallet extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id", unique = true)
    private User user;

    @Column(nullable = false)
    private BigDecimal balance;

    public static Wallet create(User user, BigDecimal balance) {
        Wallet wallet = new Wallet();
        wallet.user = user;
        wallet.balance = balance;
        return wallet;
    }

    public void withdraw(BigDecimal amount) {
        if (this.balance.compareTo(amount) < 0) {
            throw new IllegalArgumentException("잔액 부족");
        }
        this.balance = this.balance.subtract(amount);
    }
}
