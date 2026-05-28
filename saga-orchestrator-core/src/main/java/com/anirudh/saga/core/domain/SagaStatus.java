package com.anirudh.saga.core.domain;

public enum SagaStatus {
    STARTED,
    IN_PROGRESS,
    COMPLETED,
    COMPENSATING,
    COMPENSATED,
    SUSPENDED,
    FAILED
}
