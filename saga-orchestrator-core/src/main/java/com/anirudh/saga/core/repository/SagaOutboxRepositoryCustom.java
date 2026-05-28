package com.anirudh.saga.core.repository;

public interface SagaOutboxRepositoryCustom {

    /**
     * Max {@code retryCount} across all PENDING outbox entries — feeds the
     * {@code saga.outbox.max.retry.count} gauge (P2-007 AC-4). Returns 0
     * when the collection has no PENDING entries (avoids null in metrics).
     */
    int findMaxRetryCountAmongPending();
}
