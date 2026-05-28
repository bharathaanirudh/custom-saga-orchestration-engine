package com.anirudh.saga.core.audit;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Append-only event record for a saga (P2-016 / E2-11).
 *
 * <p>This collection (`saga_execution_log`) IS the engine's immutable event store —
 * P2-016 hardened it rather than spawning a parallel `saga_events` collection (the
 * story's literal wording), because two append-only streams recording the same
 * transitions would drift. Per EXECUTION-PLAN §1.1, the event store builds on this
 * log, doesn't replace it.
 *
 * <p>Immutability is enforced by construction (only {@code CheckpointStore.logEvent}
 * inserts; nothing updates) plus a `$jsonSchema` validator added in Mongock V004.
 * The optional {@code instanceSnapshot} captures full saga state at the moment of the
 * event — the foundation for replay (P2-017 / P2-027).
 */
@Document("saga_execution_log")
public class SagaExecutionLog {

    @Id
    private String id;
    private String sagaId;
    private String stepName;
    private String event;
    private String data;
    private Instant timestamp;
    private String actor;                       // P2-016: who produced the event (default "ENGINE")
    private Map<String, Object> instanceSnapshot; // P2-016: saga state at this point (for replay); nullable

    public SagaExecutionLog() {}

    public static SagaExecutionLog of(String sagaId, String stepName, String event, String data, Instant now) {
        return of(sagaId, stepName, event, data, now, "ENGINE", null);
    }

    public static SagaExecutionLog of(String sagaId, String stepName, String event, String data,
                                      Instant now, String actor, Map<String, Object> instanceSnapshot) {
        SagaExecutionLog log = new SagaExecutionLog();
        log.id = UUID.randomUUID().toString();
        log.sagaId = sagaId;
        log.stepName = stepName;
        log.event = event;
        log.data = data;
        log.timestamp = now;
        log.actor = actor;
        log.instanceSnapshot = instanceSnapshot;
        return log;
    }

    public String getId() { return id; }
    public String getSagaId() { return sagaId; }
    public void setSagaId(String sagaId) { this.sagaId = sagaId; }
    public String getStepName() { return stepName; }
    public void setStepName(String stepName) { this.stepName = stepName; }
    public String getEvent() { return event; }
    public void setEvent(String event) { this.event = event; }
    public String getData() { return data; }
    public void setData(String data) { this.data = data; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
    public String getActor() { return actor; }
    public void setActor(String actor) { this.actor = actor; }
    public Map<String, Object> getInstanceSnapshot() { return instanceSnapshot; }
    public void setInstanceSnapshot(Map<String, Object> instanceSnapshot) { this.instanceSnapshot = instanceSnapshot; }
}
