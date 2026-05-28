package com.anirudh.saga.core.executor;

import com.anirudh.saga.core.domain.StepType;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class StepExecutorRegistry {

    private final Map<StepType, StepExecutor> executors;

    public StepExecutorRegistry(List<StepExecutor> executorList) {
        this.executors = executorList.stream()
                .collect(Collectors.toMap(StepExecutor::supports, Function.identity()));
    }

    public StepExecutor getExecutor(StepType type) {
        StepExecutor executor = executors.get(type);
        if (executor == null) throw new IllegalStateException("No executor found for step type: " + type);
        return executor;
    }
}
