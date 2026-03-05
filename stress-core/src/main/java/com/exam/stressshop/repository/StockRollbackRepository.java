package com.exam.stressshop.repository;

import com.exam.stressshop.domain.stockrollback.StockRollbackHistory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StockRollbackRepository extends JpaRepository<StockRollbackHistory, String> {

}
