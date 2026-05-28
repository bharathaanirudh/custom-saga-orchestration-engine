package com.anirudh.saga.core.loader;

import com.anirudh.saga.core.domain.SagaDefinition;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;
import com.anirudh.saga.core.exception.SagaDefinitionInvalidException;

import java.util.HashSet;
import java.util.Set;

public class SagaDefinitionValidator {

    public void validate(SagaDefinition definition) {
        validateNameNotBlank(definition);
        validateTimeoutPositive(definition);
        validateStepsNotEmpty(definition);
        validateUniqueStepNames(definition);
        for (StepDefinition step : definition.steps()) {
            validateKafkaHasTopic(definition.name(), step);
            validateHttpHasUrl(definition.name(), step);
            validateCompensationCompleteness(definition.name(), step);
        }
    }

    private void validateNameNotBlank(SagaDefinition def) {
        if (def.name() == null || def.name().isBlank()) {
            throw new SagaDefinitionInvalidException("Saga definition name must not be blank");
        }
    }

    private void validateTimeoutPositive(SagaDefinition def) {
        if (def.timeoutMinutes() <= 0) {
            throw new SagaDefinitionInvalidException(
                    "Saga [" + def.name() + "] timeoutMinutes must be positive, got: " + def.timeoutMinutes());
        }
    }

    private void validateStepsNotEmpty(SagaDefinition def) {
        if (def.steps() == null || def.steps().isEmpty()) {
            throw new SagaDefinitionInvalidException("Saga [" + def.name() + "] must have at least one step");
        }
    }

    private void validateUniqueStepNames(SagaDefinition def) {
        Set<String> seen = new HashSet<>();
        for (StepDefinition step : def.steps()) {
            if (!seen.add(step.name())) {
                throw new SagaDefinitionInvalidException(
                        "Saga [" + def.name() + "] has duplicate step name: " + step.name());
            }
        }
    }

    private void validateKafkaHasTopic(String sagaName, StepDefinition step) {
        if (step.type() == StepType.KAFKA && step.resolvedTopic() == null) {
            throw new SagaDefinitionInvalidException(
                    "Saga [" + sagaName + "] step [" + step.name() + "] is KAFKA type but has no topic or module");
        }
    }

    private void validateHttpHasUrl(String sagaName, StepDefinition step) {
        if (step.type() == StepType.HTTP && (step.url() == null || step.url().isBlank())) {
            throw new SagaDefinitionInvalidException(
                    "Saga [" + sagaName + "] step [" + step.name() + "] is HTTP type but has no url");
        }
    }

    private void validateCompensationCompleteness(String sagaName, StepDefinition step) {
        if (step.compensationAction() == null || step.compensationAction().isBlank()) return;
        if (step.type() == StepType.KAFKA && step.resolvedCompensationTopic() == null) {
            throw new SagaDefinitionInvalidException(
                    "Saga [" + sagaName + "] step [" + step.name() + "] has compensationAction but no compensationTopic or module");
        }
        if (step.type() == StepType.HTTP && (step.compensationUrl() == null || step.compensationUrl().isBlank())) {
            throw new SagaDefinitionInvalidException(
                    "Saga [" + sagaName + "] step [" + step.name() + "] has compensationAction but no compensationUrl");
        }
    }
}
