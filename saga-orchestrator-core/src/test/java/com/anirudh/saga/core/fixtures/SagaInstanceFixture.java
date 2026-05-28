package com.anirudh.saga.core.fixtures;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.SagaStatus;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class SagaInstanceFixture {

    public static SagaInstance started() {
        return SagaInstance.create("test-saga", Map.of("orderId", "ORD-001"), idempotencyKey(), 30, Instant.now());
    }

    public static SagaInstance inProgress() {
        SagaInstance instance = started();
        instance.setStatus(SagaStatus.IN_PROGRESS);
        return instance;
    }

    public static SagaInstance inProgressAtStep(int step) {
        SagaInstance instance = inProgress();
        instance.setCurrentStep(step);
        return instance;
    }

    public static SagaInstance suspended() {
        SagaInstance instance = inProgress();
        instance.setStatus(SagaStatus.SUSPENDED);
        instance.setSuspendedAt(Instant.now());
        return instance;
    }

    public static SagaInstance timedOut(int timeoutMinutesAgo) {
        SagaInstance instance = inProgress();
        instance.setTimeoutAt(Instant.now().minusSeconds((long) timeoutMinutesAgo * 60));
        return instance;
    }

    public static SagaInstance withContext(String key, Object value) {
        SagaInstance instance = inProgress();
        instance.getContext().put(key, value);
        return instance;
    }

    private static String idempotencyKey() {
        return "key-" + UUID.randomUUID();
    }
}
