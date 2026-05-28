package com.anirudh.saga.core.lock;

import com.anirudh.saga.core.exception.SagaLockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Manages aggregate-level locks for sagas.
 *
 * Before a saga starts, it can acquire a lock on a target entity.
 * Other sagas attempting to lock the same entity will fail fast.
 * Locks auto-expire after a configurable duration (default: saga timeout).
 *
 * Usage in YAML:
 *   lockTarget: "order:${orderId}"
 *
 * Usage programmatically:
 *   sagaLockManager.acquire("order", orderId, sagaId, sagaType, Duration.ofMinutes(30));
 */
@Component
public class SagaLockManager {

    private static final Logger log = LoggerFactory.getLogger(SagaLockManager.class);
    private static final Duration DEFAULT_LOCK_TTL = Duration.ofMinutes(30);

    private final SagaLockRepository lockRepository;
    private final Clock clock;

    public SagaLockManager(SagaLockRepository lockRepository, Clock clock) {
        this.lockRepository = lockRepository;
        this.clock = clock;
    }

    /**
     * Acquire a lock on a target entity. Fails fast if another saga holds it.
     * @throws SagaLockException if lock already held by another saga
     */
    public void acquire(String targetType, String targetId, String sagaId, String sagaType, Duration ttl) {
        String lockKey = SagaLock.buildKey(targetType, targetId);
        Instant now = Instant.now(clock);
        Instant expiresAt = now.plus(ttl != null ? ttl : DEFAULT_LOCK_TTL);

        try {
            SagaLock lock = new SagaLock(lockKey, sagaId, sagaType, now, expiresAt);
            lockRepository.save(lock);
            log.info("[sagaId={}] Lock acquired: {}", sagaId, lockKey);
        } catch (DuplicateKeyException e) {
            // Lock exists — check if expired
            SagaLock existing = lockRepository.findById(lockKey).orElse(null);
            if (existing != null && existing.getExpiresAt().isBefore(now)) {
                // Expired lock — replace it
                lockRepository.deleteById(lockKey);
                SagaLock lock = new SagaLock(lockKey, sagaId, sagaType, now, expiresAt);
                lockRepository.save(lock);
                log.info("[sagaId={}] Replaced expired lock: {} (was held by sagaId={})",
                        sagaId, lockKey, existing.getSagaId());
            } else {
                String holderId = existing != null ? existing.getSagaId() : "unknown";
                log.warn("[sagaId={}] Lock denied: {} held by sagaId={}", sagaId, lockKey, holderId);
                throw new SagaLockException(
                        "Lock [" + lockKey + "] held by saga [" + holderId + "]");
            }
        }
    }

    public void acquire(String targetType, String targetId, String sagaId, String sagaType) {
        acquire(targetType, targetId, sagaId, sagaType, DEFAULT_LOCK_TTL);
    }

    /**
     * Release all locks held by a saga. Called on COMPLETED, COMPENSATED, or FAILED.
     */
    public void releaseAll(String sagaId) {
        lockRepository.deleteBySagaId(sagaId);
        log.info("[sagaId={}] All locks released", sagaId);
    }

    /**
     * Cleanup expired locks — runs every 5 minutes.
     */
    @Scheduled(fixedDelayString = "${saga.lock.cleanup-interval-ms:300000}")
    public void cleanupExpiredLocks() {
        List<SagaLock> expired = lockRepository.findExpiredLocks(Instant.now(clock));
        if (expired.isEmpty()) return;
        log.warn("Cleaning up {} expired saga locks", expired.size());
        for (SagaLock lock : expired) {
            lockRepository.deleteById(lock.getLockKey());
            log.warn("[sagaId={}] Expired lock cleaned: {}", lock.getSagaId(), lock.getLockKey());
        }
    }
}
