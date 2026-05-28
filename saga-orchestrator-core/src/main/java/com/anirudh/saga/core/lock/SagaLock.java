package com.anirudh.saga.core.lock;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Saga lock on an aggregate entity. Prevents concurrent sagas from operating
 * on the same entity (e.g., same orderId, same userId).
 *
 * Lock key format: {targetType}:{targetId}  e.g. "order:ORD-123"
 * Only one saga can hold a lock on a given target at a time.
 */
@Document("saga_locks")
public class SagaLock {

    @Id
    private String lockKey;       // unique: targetType:targetId
    private String sagaId;        // saga that holds the lock
    private String sagaType;
    private Instant lockedAt;
    private Instant expiresAt;    // auto-expire if saga dies without releasing

    public SagaLock() {}

    public SagaLock(String lockKey, String sagaId, String sagaType, Instant lockedAt, Instant expiresAt) {
        this.lockKey = lockKey;
        this.sagaId = sagaId;
        this.sagaType = sagaType;
        this.lockedAt = lockedAt;
        this.expiresAt = expiresAt;
    }

    public static String buildKey(String targetType, String targetId) {
        return targetType + ":" + targetId;
    }

    public String getLockKey() { return lockKey; }
    public String getSagaId() { return sagaId; }
    public String getSagaType() { return sagaType; }
    public Instant getLockedAt() { return lockedAt; }
    public Instant getExpiresAt() { return expiresAt; }
}
