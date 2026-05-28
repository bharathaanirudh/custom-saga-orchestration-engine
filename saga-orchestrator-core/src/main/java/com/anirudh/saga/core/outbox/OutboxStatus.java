package com.anirudh.saga.core.outbox;

public enum OutboxStatus {
    PENDING,
    CLAIMED,
    PUBLISHED
}
