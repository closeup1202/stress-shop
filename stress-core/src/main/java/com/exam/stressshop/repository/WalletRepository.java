package com.exam.stressshop.repository;

import com.exam.stressshop.domain.wallet.Wallet;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.math.BigDecimal;

public interface WalletRepository extends JpaRepository<Wallet, Long> {

    @Modifying
    @Query("""
    UPDATE Wallet w
    SET w.balance = w.balance - :amount
    WHERE w.user.id = :userId
    AND w.balance >= :amount
""")
    int decreaseBalance(
            @Param("userId") Long userId,
            @Param("amount") BigDecimal amount
    );
}
