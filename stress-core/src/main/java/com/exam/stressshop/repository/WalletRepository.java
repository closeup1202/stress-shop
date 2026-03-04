package com.exam.stressshop.repository;

import com.exam.stressshop.domain.wallet.Wallet;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WalletRepository extends JpaRepository<Wallet, Long> {
}
