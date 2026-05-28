package com.anirudh.saga.core.unit.engine;

import com.anirudh.saga.core.domain.SagaDefinition;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;
import com.anirudh.saga.core.exception.SagaDefinitionInvalidException;
import com.anirudh.saga.core.fixtures.SagaDefinitionFixture;
import com.anirudh.saga.core.loader.SagaDefinitionValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class SagaDefinitionValidatorTest {

    SagaDefinitionValidator validator;

    @BeforeEach
    void setUp() {
        validator = new SagaDefinitionValidator();
    }

    @Test
    void validate_validDefinition_doesNotThrow() {
        assertThatCode(() -> validator.validate(SagaDefinitionFixture.twoStepKafkaSaga()))
                .doesNotThrowAnyException();
    }

    @Test
    void validate_emptyName_throws() {
        SagaDefinition def = new SagaDefinition("", 30, List.of(
                SagaDefinitionFixture.kafkaStep("s", "A", "t", null, null)));
        assertThatThrownBy(() -> validator.validate(def))
                .isInstanceOf(SagaDefinitionInvalidException.class)
                .hasMessageContaining("name");
    }

    @Test
    void validate_noSteps_throws() {
        SagaDefinition def = new SagaDefinition("my-saga", 30, Collections.emptyList());
        assertThatThrownBy(() -> validator.validate(def))
                .isInstanceOf(SagaDefinitionInvalidException.class)
                .hasMessageContaining("step");
    }

    @Test
    void validate_negativeTimeout_throws() {
        SagaDefinition def = new SagaDefinition("my-saga", -1, List.of(
                SagaDefinitionFixture.kafkaStep("s", "A", "t", null, null)));
        assertThatThrownBy(() -> validator.validate(def))
                .isInstanceOf(SagaDefinitionInvalidException.class)
                .hasMessageContaining("timeout");
    }

    @Test
    void validate_duplicateStepNames_throws() {
        SagaDefinition def = new SagaDefinition("my-saga", 30, List.of(
                SagaDefinitionFixture.kafkaStep("step-one", "A", "t", null, null),
                SagaDefinitionFixture.kafkaStep("step-one", "B", "t", null, null)));
        assertThatThrownBy(() -> validator.validate(def))
                .isInstanceOf(SagaDefinitionInvalidException.class)
                .hasMessageContaining("duplicate");
    }

    @Test
    void validate_kafkaStepWithoutTopic_throws() {
        SagaDefinition def = new SagaDefinition("my-saga", 30, List.of(
                new StepDefinition("step", StepType.KAFKA, "ACTION", null, null, null, null, null, null, 3, 30, null)));
        assertThatThrownBy(() -> validator.validate(def))
                .isInstanceOf(SagaDefinitionInvalidException.class)
                .hasMessageContaining("topic");
    }

    @Test
    void validate_httpStepWithoutUrl_throws() {
        SagaDefinition def = new SagaDefinition("my-saga", 30, List.of(
                new StepDefinition("step", StepType.HTTP, "ACTION", null, null, null, null, null, null, 2, 10, null)));
        assertThatThrownBy(() -> validator.validate(def))
                .isInstanceOf(SagaDefinitionInvalidException.class)
                .hasMessageContaining("url");
    }

    @Test
    void validate_compensationActionWithoutTopic_throws() {
        SagaDefinition def = new SagaDefinition("my-saga", 30, List.of(
                new StepDefinition("step", StepType.KAFKA, "ACTION", null, "my-topic", null, "COMP", null, null, 3, 30, null)));
        assertThatThrownBy(() -> validator.validate(def))
                .isInstanceOf(SagaDefinitionInvalidException.class)
                .hasMessageContaining("compensationTopic");
    }

    @Test
    void validate_nullCompensationAction_doesNotThrow() {
        // Forward-only step is valid
        SagaDefinition def = new SagaDefinition("my-saga", 30, List.of(
                SagaDefinitionFixture.kafkaStep("step", "ACTION", "topic", null, null)));
        assertThatCode(() -> validator.validate(def)).doesNotThrowAnyException();
    }
}
