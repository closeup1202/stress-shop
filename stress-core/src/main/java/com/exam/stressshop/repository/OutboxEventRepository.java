package com.exam.stressshop.repository;

import com.exam.stressshop.domain.outbox.OutboxEvent;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {
    // FOR UPDATE SKIP LOCKED: 다중 인스턴스 배포 시 같은 이벤트 중복 발행 방지
    // SKIP LOCKED 이유: 인스턴스가 2개 이상 뜰 때 동일한 Outbox 레코드를 두 폴러가 동시에 읽으면 Kafka에 중복 발행된다.
    // lock.timeout=0은 락 획득 즉시 실패(skip)하게 하여 다른 인스턴스가 처리 중인 레코드를 건너뛴다.
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "0"))
    @Query("SELECT o FROM OutboxEvent o WHERE o.status = 'PENDING' ORDER BY o.createdAt ASC")
    List<OutboxEvent> findPendingWithLock(Pageable pageable);
}
