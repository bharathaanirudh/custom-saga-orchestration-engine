package com.anirudh.saga.core.executor;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.domain.StepDefinition;
import com.anirudh.saga.core.domain.StepType;

public interface StepExecutor {
    StepType supports();
    StepResult execute(SagaInstance instance, StepDefinition step);
    void compensate(SagaInstance instance, StepDefinition step);
}
