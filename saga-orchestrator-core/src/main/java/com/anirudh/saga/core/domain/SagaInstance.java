package com.anirudh.saga.core.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.*;

@Document("saga_instances")
public class SagaInstance {

    @Id
    private String sagaId;
    private String sagaType;
    private SagaStatus status;
    private String idempotencyKey;
    private String idempotencyPayloadHash;
    private Map<String, Object> payload = new HashMap<>();
    private Map<String, Object> context = new HashMap<>();
    private int currentStep;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant timeoutAt;
    private Instant currentStepStartedAt;
    private Instant suspendedAt;
    private List<String> failedCompensations = new ArrayList<>();

    @Version
    private Long version;

    public SagaInstance() {}

    public static SagaInstance create(String sagaType, Map<String, Object> payload,
                                       String idempotencyKey, int timeoutMinutes, Instant now) {
        SagaInstance instance = new SagaInstance();
        instance.sagaId = UUID.randomUUID().toString();
        instance.sagaType = sagaType;
        instance.status = SagaStatus.STARTED;
        instance.payload = payload != null ? payload : new HashMap<>();
        instance.context = new HashMap<>();
        instance.idempotencyKey = idempotencyKey;
        instance.currentStep = 0;
        instance.createdAt = now;
        instance.updatedAt = now;
        instance.timeoutAt = now.plusSeconds((long) timeoutMinutes * 60);
        instance.failedCompensations = new ArrayList<>();
        return instance;
    }

    public void touch(Instant now) {
        this.updatedAt = now;
    }

    public void addFailedCompensation(String stepName) {
        failedCompensations.add(stepName);
    }

    // Getters and setters
    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }
    public String getSagaType() { return sagaType; }
    public void setSagaType(String sagaType) { this.sagaType = sagaType; }
    public SagaStatus getStatus() { return status; }
    public void setStatus(SagaStatus status) { this.status = status; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public String getIdempotencyPayloadHash() { return idempotencyPayloadHash; }
    public void setIdempotencyPayloadHash(String idempotencyPayloadHash) { this.idempotencyPayloadHash = idempotencyPayloadHash; }
    public Map<String, Object> getPayload() { return payload; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }
    public Map<String, Object> getContext() { return context; }
    public void setContext(Map<String, Object> context) { this.context = context; }
    public int getCurrentStep() { return currentStep; }
    public void setCurrentStep(int currentStep) { this.currentStep = currentStep; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public Instant getTimeoutAt() { return timeoutAt; }
    public void setTimeoutAt(Instant timeoutAt) { this.timeoutAt = timeoutAt; }
    public Instant getCurrentStepStartedAt() { return currentStepStartedAt; }
    public void setCurrentStepStartedAt(Instant currentStepStartedAt) { this.currentStepStartedAt = currentStepStartedAt; }
    public Instant getSuspendedAt() { return suspendedAt; }
    public void setSuspendedAt(Instant suspendedAt) { this.suspendedAt = suspendedAt; }
    public List<String> getFailedCompensations() { return failedCompensations; }
    public void setFailedCompensations(List<String> failedCompensations) { this.failedCompensations = failedCompensations; }
    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
}
