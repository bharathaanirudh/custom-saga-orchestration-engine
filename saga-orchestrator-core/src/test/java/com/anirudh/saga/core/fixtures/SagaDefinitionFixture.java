package com.anirudh.saga.core.fixtures;

import com.anirudh.saga.core.domain.SagaDefinition;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;

import java.util.List;

public class SagaDefinitionFixture {

    public static SagaDefinition twoStepKafkaSaga() {
        return new SagaDefinition(
                "test-saga",
                30,
                List.of(
                        kafkaStep("step-one", "ACTION_ONE", "topic-one", "COMPENSATE_ONE", "topic-one"),
                        kafkaStep("step-two", "ACTION_TWO", "topic-two", null, null)
                )
        );
    }

    public static SagaDefinition singleKafkaStep() {
        return new SagaDefinition(
                "single-step-saga",
                10,
                List.of(kafkaStep("only-step", "DO_THING", "my-topic", "UNDO_THING", "my-topic"))
        );
    }

    public static SagaDefinition threeStepSagaWithCompensation() {
        return new SagaDefinition(
                "three-step-saga",
                30,
                List.of(
                        kafkaStep("step-one",   "ACTION_ONE",   "topic-a", "COMP_ONE",   "topic-a"),
                        kafkaStep("step-two",   "ACTION_TWO",   "topic-b", "COMP_TWO",   "topic-b"),
                        kafkaStep("step-three", "ACTION_THREE", "topic-c", "COMP_THREE", "topic-c")
                )
        );
    }

    public static StepDefinition kafkaStep(String name, String action, String topic,
                                            String compensationAction, String compensationTopic) {
        return new StepDefinition(name, StepType.KAFKA, action, null, topic, null,
                compensationAction, compensationTopic, null, 3, 30, null);
    }

    public static StepDefinition httpStep(String name, String action, String url,
                                           String compensationAction, String compensationUrl) {
        return new StepDefinition(name, StepType.HTTP, action, null, null, url,
                compensationAction, null, compensationUrl, 2, 10, null);
    }
}
