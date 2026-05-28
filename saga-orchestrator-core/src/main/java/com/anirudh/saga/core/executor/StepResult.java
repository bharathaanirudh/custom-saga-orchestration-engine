package com.anirudh.saga.core.executor;

/**
 * Result of a step execution — returned by StepExecutor to SagaOrchestrator.
 * Kafka steps always return DISPATCHED (async). HTTP steps return the actual result.
 */
public record StepResult(
        Type type,
        String failureType,
        Object data,
        String error
) {
    public enum Type { DISPATCHED, SUCCESS, BUSINESS_FAILURE, TECHNICAL_FAILURE }

    public static StepResult dispatched() {
        return new StepResult(Type.DISPATCHED, null, null, null);
    }

    public static StepResult success(Object data) {
        return new StepResult(Type.SUCCESS, null, data, null);
    }

    public static StepResult businessFailure(String error) {
        return new StepResult(Type.BUSINESS_FAILURE, "BUSINESS", null, error);
    }

    public static StepResult technicalFailure(String error) {
        return new StepResult(Type.TECHNICAL_FAILURE, "TECHNICAL", null, error);
    }

    public boolean isDispatched() { return type == Type.DISPATCHED; }
    public boolean isSuccess() { return type == Type.SUCCESS; }
    public boolean isBusinessFailure() { return type == Type.BUSINESS_FAILURE; }
    public boolean isTechnicalFailure() { return type == Type.TECHNICAL_FAILURE; }
}
