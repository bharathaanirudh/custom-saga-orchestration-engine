package com.anirudh.saga.core.outbox;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.UUID;

@Document("saga_outbox")
public class OutboxEntry {

    @Id
    private String id;
    private String sagaId;
    private String stepId;
    private String topic;
    private String messageKey;
    private String payload;
    private OutboxStatus status;
    private Instant createdAt;
    private String claimedBy;
    private Instant claimedAt;
    private int retryCount;
    private String lastError;
    private Instant lastErrorAt;

    public OutboxEntry() {}

    public static OutboxEntry create(String sagaId, String stepId, String topic, String messageKey, String payload, Instant now) {
        OutboxEntry entry = new OutboxEntry();
        entry.id = UUID.randomUUID().toString();
        entry.sagaId = sagaId;
        entry.stepId = stepId;
        entry.topic = topic;
        entry.messageKey = messageKey;
        entry.payload = payload;
        entry.status = OutboxStatus.PENDING;
        entry.createdAt = now;
        return entry;
    }

    public String getId() { return id; }
    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }
    public String getStepId() { return stepId; }
    public void setStepId(String stepId) { this.stepId = stepId; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public String getMessageKey() { return messageKey; }
    public void setMessageKey(String messageKey) { this.messageKey = messageKey; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public OutboxStatus getStatus() { return status; }
    public void setStatus(OutboxStatus status) { this.status = status; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public String getClaimedBy() { return claimedBy; }
    public void setClaimedBy(String claimedBy) { this.claimedBy = claimedBy; }
    public Instant getClaimedAt() { return claimedAt; }
    public void setClaimedAt(Instant claimedAt) { this.claimedAt = claimedAt; }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
    public Instant getLastErrorAt() { return lastErrorAt; }
    public void setLastErrorAt(Instant lastErrorAt) { this.lastErrorAt = lastErrorAt; }
}
