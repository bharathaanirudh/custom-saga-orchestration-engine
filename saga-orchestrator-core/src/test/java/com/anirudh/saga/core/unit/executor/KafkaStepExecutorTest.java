package com.anirudh.saga.core.unit.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.anirudh.saga.core.executor.KafkaStepExecutor;
import com.anirudh.saga.core.executor.StepResult;
import com.anirudh.saga.core.fixtures.SagaDefinitionFixture;
import com.anirudh.saga.core.fixtures.SagaInstanceFixture;
import com.anirudh.saga.core.outbox.OutboxWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaStepExecutorTest {

    @Mock OutboxWriter outboxWriter;

    KafkaStepExecutor executor;
    Clock fixedClock;

    @BeforeEach
    void setUp() {
        fixedClock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        executor = new KafkaStepExecutor(outboxWriter, new ObjectMapper(), fixedClock);
    }

    @Test
    void execute_returnsDispatched() {
        StepResult result = executor.execute(SagaInstanceFixture.inProgress(),
                SagaDefinitionFixture.kafkaStep("step", "ACTION", "topic", null, null));
        assertThat(result.isDispatched()).isTrue();
    }

    @Test
    void execute_writesToOutboxAtomic() {
        executor.execute(SagaInstanceFixture.inProgress(),
                SagaDefinitionFixture.kafkaStep("reserve", "RESERVE", "inventory-commands", "RELEASE", "inventory-commands"));
        verify(outboxWriter).writeAtomic(any(), eq("reserve"), eq("inventory-commands"), anyString(), anyString(), any());
    }

    @Test
    void execute_payloadContainsAction() {
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        executor.execute(SagaInstanceFixture.inProgress(),
                SagaDefinitionFixture.kafkaStep("step", "RESERVE_INVENTORY", "topic", null, null));
        verify(outboxWriter).writeAtomic(any(), any(), any(), any(), payloadCaptor.capture(), any());
        assertThat(payloadCaptor.getValue()).contains("RESERVE_INVENTORY");
    }

    @Test
    void compensate_writesToCompensationTopic() {
        executor.compensate(SagaInstanceFixture.inProgress(),
                SagaDefinitionFixture.kafkaStep("step", "ACTION", "forward", "COMP", "comp-topic"));
        verify(outboxWriter).write(anyString(), contains("compensation"), eq("comp-topic"), any(), anyString(), any());
    }
}
