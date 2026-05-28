package com.anirudh.saga.core.unit.metrics;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;
import com.anirudh.saga.core.fixtures.SagaInstanceFixture;
import com.anirudh.saga.core.metrics.SagaMetrics;
import com.anirudh.saga.sdk.contract.FailureType;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SagaMetrics} (P2-025).
 *
 * Verifies:
 *  - counters are tagged with sagaType (and stepName/failureType where applicable)
 *  - cardinality cap falls back to "OTHER"
 *  - same tag set re-uses the registered counter
 */
class SagaMetricsTest {

    private MeterRegistry registry;
    private SagaMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new SagaMetrics(registry, 100);
    }

    @Test
    void started_taggedBySagaType() {
        SagaInstance instance = SagaInstanceFixture.started();
        instance.setSagaType("ORDER_PLACEMENT_SAGA");

        metrics.recordStarted(instance);

        assertThat(registry.find("saga.started").tag("sagaType", "ORDER_PLACEMENT_SAGA").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void completed_taggedBySagaType() {
        SagaInstance instance = SagaInstanceFixture.started();
        instance.setSagaType("ORDER_PLACEMENT_SAGA");

        metrics.recordCompleted(instance);

        assertThat(registry.find("saga.completed").tag("sagaType", "ORDER_PLACEMENT_SAGA").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void stepFailed_taggedByStepNameAndFailureType() {
        SagaInstance instance = SagaInstanceFixture.started();
        instance.setSagaType("ORDER_PLACEMENT_SAGA");
        StepDefinition step = step("charge-payment");

        metrics.recordStepFailed(instance, step, FailureType.BUSINESS);

        assertThat(registry.find("saga.step.failed")
                .tag("sagaType", "ORDER_PLACEMENT_SAGA")
                .tag("stepName", "charge-payment")
                .tag("failureType", "BUSINESS")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void stepExecuted_taggedByStepName() {
        SagaInstance instance = SagaInstanceFixture.started();
        instance.setSagaType("ORDER_PLACEMENT_SAGA");
        StepDefinition step = step("ship-order");

        metrics.recordStepExecuted(instance, step);

        assertThat(registry.find("saga.step.executed")
                .tag("sagaType", "ORDER_PLACEMENT_SAGA")
                .tag("stepName", "ship-order")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void cardinalityOverflow_fallsBackToOther() {
        metrics = new SagaMetrics(registry, 2); // cap at 2

        SagaInstance a = saga("TYPE_A");
        SagaInstance b = saga("TYPE_B");
        SagaInstance c = saga("TYPE_C");

        assertThat(metrics.boundedSagaType(a)).isEqualTo("TYPE_A");
        assertThat(metrics.boundedSagaType(b)).isEqualTo("TYPE_B");
        assertThat(metrics.boundedSagaType(c)).isEqualTo("OTHER");
        // Already-seen types still pass through after cap is reached.
        assertThat(metrics.boundedSagaType(a)).isEqualTo("TYPE_A");
    }

    @Test
    void multipleIncrements_reuseSameCounter() {
        SagaInstance instance = saga("ORDER_PLACEMENT_SAGA");

        metrics.recordStarted(instance);
        metrics.recordStarted(instance);
        metrics.recordStarted(instance);

        assertThat(registry.getMeters()).filteredOn(m -> "saga.started".equals(m.getId().getName())).hasSize(1);
        assertThat(registry.find("saga.started").tag("sagaType", "ORDER_PLACEMENT_SAGA").counter().count())
                .isEqualTo(3.0);
    }

    @Test
    void nullSagaType_fallsBackToUnknown() {
        SagaInstance instance = saga(null);

        metrics.recordStarted(instance);

        assertThat(registry.find("saga.started").tag("sagaType", "unknown").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void resetCardinalityCache_clearsSeenTypes() {
        metrics = new SagaMetrics(registry, 1);
        metrics.boundedSagaType(saga("TYPE_A"));
        assertThat(metrics.boundedSagaType(saga("TYPE_B"))).isEqualTo("OTHER");

        metrics.resetCardinalityCache();

        assertThat(metrics.boundedSagaType(saga("TYPE_B"))).isEqualTo("TYPE_B");
    }

    private static SagaInstance saga(String type) {
        SagaInstance instance = SagaInstanceFixture.started();
        instance.setSagaType(type);
        return instance;
    }

    private static StepDefinition step(String name) {
        return new StepDefinition(name, StepType.HTTP, "ACT", null, null,
                "http://example/" + name, null, null, null, 0, 0, null);
    }
}
