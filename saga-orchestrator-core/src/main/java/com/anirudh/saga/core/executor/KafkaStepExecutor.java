package com.anirudh.saga.core.executor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;
import com.anirudh.saga.core.exception.SagaExecutionException;
import com.anirudh.saga.core.outbox.OutboxWriter;
import com.anirudh.saga.sdk.contract.SagaCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;

@Component
public class KafkaStepExecutor implements StepExecutor {

    private static final Logger log = LoggerFactory.getLogger(KafkaStepExecutor.class);

    private final OutboxWriter outboxWriter;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public KafkaStepExecutor(OutboxWriter outboxWriter, ObjectMapper objectMapper, Clock clock) {
        this.outboxWriter = outboxWriter;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Override
    public StepType supports() {
        return StepType.KAFKA;
    }

    @Override
    public StepResult execute(SagaInstance instance, StepDefinition step) {
        log.info("[sagaId={}] Dispatching Kafka step={} action={} topic={}",
                instance.getSagaId(), step.name(), step.action(), step.resolvedTopic());
        SagaCommand command = new SagaCommand(instance.getSagaId(), step.name(), step.action(), instance.getPayload());
        String payload = serialize(command);
        outboxWriter.writeAtomic(instance, step.name(), step.resolvedTopic(), instance.getSagaId(), payload, Instant.now(clock));
        return StepResult.dispatched(); // Async — reply comes via ReplyCorrelator
    }

    @Override
    public void compensate(SagaInstance instance, StepDefinition step) {
        log.info("[sagaId={}] Dispatching Kafka compensation step={} action={}",
                instance.getSagaId(), step.name(), step.compensationAction());
        SagaCommand command = new SagaCommand(instance.getSagaId(), step.name(), step.compensationAction(), instance.getContext());
        String payload = serialize(command);
        outboxWriter.write(instance.getSagaId(), step.name() + "-compensation",
                step.resolvedCompensationTopic(), instance.getSagaId(), payload, Instant.now(clock));
    }

    private String serialize(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new SagaExecutionException("Failed to serialize command", e);
        }
    }
}
