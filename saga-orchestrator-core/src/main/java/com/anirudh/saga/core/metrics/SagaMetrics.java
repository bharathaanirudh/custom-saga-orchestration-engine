package com.anirudh.saga.core.metrics;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.sdk.contract.FailureType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-saga-type and per-step tagged metrics (P2-025).
 *
 * Cardinality cap: distinct {@code sagaType} values are tracked in a bounded set;
 * once the cap is hit, new types fall back to {@code "OTHER"} to prevent the
 * canonical Prometheus footgun (unbounded label cardinality → metric explosion).
 */
@Component
public class SagaMetrics {

    public static final String OTHER = "OTHER";
    private static final String UNKNOWN = "unknown";

    private final MeterRegistry registry;
    private final int maxTagCardinality;
    private final Set<String> seenSagaTypes = ConcurrentHashMap.newKeySet();
    private final Timer.Builder durationBuilder;

    public SagaMetrics(MeterRegistry registry,
                       @Value("${saga.metrics.max-tag-cardinality:100}") int maxTagCardinality) {
        this.registry = registry;
        this.maxTagCardinality = maxTagCardinality;
        this.durationBuilder = Timer.builder("saga.duration")
                .description("Saga execution duration");
    }

    public void recordStarted(SagaInstance instance) { increment("saga.started", instance); }
    public void recordCompleted(SagaInstance instance) { increment("saga.completed", instance); }
    public void recordCompensated(SagaInstance instance) { increment("saga.compensated", instance); }
    public void recordFailed(SagaInstance instance) { increment("saga.failed", instance); }
    public void recordSuspended(SagaInstance instance) { increment("saga.suspended", instance); }

    public void recordStepExecuted(SagaInstance instance, StepDefinition step) {
        Counter.builder("saga.step.executed")
                .description("Total steps executed")
                .tags(stepTags(instance, step))
                .register(registry)
                .increment();
    }

    public void recordStepFailed(SagaInstance instance, StepDefinition step, FailureType failureType) {
        Counter.builder("saga.step.failed")
                .description("Total steps failed")
                .tags(stepTags(instance, step).and("failureType",
                        failureType != null ? failureType.name() : UNKNOWN))
                .register(registry)
                .increment();
    }

    /** P2-067: technical retries exhausted, declared step fallback applied instead of suspending. */
    public void recordStepFallbackApplied(SagaInstance instance, StepDefinition step) {
        Counter.builder("saga.step.fallback.applied")
                .description("Total times a declared step fallback was applied after retries exhausted")
                .tags(stepTags(instance, step))
                .register(registry)
                .increment();
    }

    /** P2-061 AC-3: reply arrived for a saga in a terminal state. */
    public void recordLateReply(String terminalStatus) {
        Counter.builder("saga.reply.late")
                .description("Replies received after saga reached a terminal state")
                .tags(Tags.of("terminalStatus", terminalStatus != null ? terminalStatus : UNKNOWN))
                .register(registry)
                .increment();
    }

    /** P2-061 AC-3: reply arrived for an unknown sagaId. */
    public void recordUnknownReply() {
        Counter.builder("saga.reply.unknown")
                .description("Replies received for non-existent sagaIds")
                .register(registry)
                .increment();
    }

    /** P2-061 AC-5: reply throttled by per-sagaId rate limiter. */
    public void recordThrottledReply() {
        Counter.builder("saga.reply.throttled")
                .description("Replies dropped by the per-sagaId rate limiter")
                .register(registry)
                .increment();
    }

    public Timer.Sample startTimer() { return Timer.start(registry); }

    public void stopTimer(Timer.Sample sample, SagaInstance instance) {
        sample.stop(durationBuilder
                .tags(Tags.of("sagaType", boundedSagaType(instance)))
                .register(registry));
    }

    /** Visible for testing. Returns {@code "OTHER"} once cardinality cap is hit. */
    public String boundedSagaType(SagaInstance instance) {
        String type = instance != null ? instance.getSagaType() : null;
        if (type == null || type.isBlank()) return UNKNOWN;
        if (seenSagaTypes.contains(type)) return type;
        if (seenSagaTypes.size() < maxTagCardinality) {
            seenSagaTypes.add(type);
            return type;
        }
        return OTHER;
    }

    /** Visible for testing — clears the cardinality cache. */
    public void resetCardinalityCache() {
        seenSagaTypes.clear();
    }

    private void increment(String name, SagaInstance instance) {
        Counter.builder(name)
                .tags(Tags.of("sagaType", boundedSagaType(instance)))
                .register(registry)
                .increment();
    }

    private Tags stepTags(SagaInstance instance, StepDefinition step) {
        return Tags.of(
                "sagaType", boundedSagaType(instance),
                "stepName", step != null ? step.name() : UNKNOWN);
    }
}
