package com.anirudh.saga.core.domain;

import java.util.List;

public record SagaDefinition(
        String name,
        int timeoutMinutes,
        List<StepDefinition> steps,
        String lockTargetType,
        String lockTargetField
) {
    /** Backward-compatible constructor without lock config */
    public SagaDefinition(String name, int timeoutMinutes, List<StepDefinition> steps) {
        this(name, timeoutMinutes, steps, null, null);
    }

    public boolean hasLockTarget() {
        return lockTargetType != null && !lockTargetType.isBlank()
                && lockTargetField != null && !lockTargetField.isBlank();
    }
}
