package com.anirudh.saga.sdk.contract;

public record SagaReply(
        String sagaId,
        String stepId,
        String status,
        FailureType failureType,
        Object data,
        String error
) {
    // ── Factory methods taking SagaCommand (preferred — SDK Approach B) ──────

    public static SagaReply success(SagaCommand command, Object data) {
        return new SagaReply(command.sagaId(), command.stepId(), "SUCCESS", null, data, null);
    }

    public static SagaReply businessFailure(SagaCommand command, String reason) {
        return new SagaReply(command.sagaId(), command.stepId(), "BUSINESS_FAILURE", FailureType.BUSINESS, null, reason);
    }

    public static SagaReply technicalFailure(SagaCommand command, String reason) {
        return new SagaReply(command.sagaId(), command.stepId(), "TECHNICAL_FAILURE", FailureType.TECHNICAL, null, reason);
    }

    // ── Factory methods taking raw strings (used by core internals) ──────────

    public static SagaReply success(String sagaId, String stepId, Object data) {
        return new SagaReply(sagaId, stepId, "SUCCESS", null, data, null);
    }

    public static SagaReply businessFailure(String sagaId, String stepId, String reason) {
        return new SagaReply(sagaId, stepId, "BUSINESS_FAILURE", FailureType.BUSINESS, null, reason);
    }

    public static SagaReply technicalFailure(String sagaId, String stepId, String reason) {
        return new SagaReply(sagaId, stepId, "TECHNICAL_FAILURE", FailureType.TECHNICAL, null, reason);
    }

    public boolean isSuccess() { return "SUCCESS".equals(status); }
    public boolean isBusinessFailure() { return FailureType.BUSINESS == failureType; }
    public boolean isTechnicalFailure() { return FailureType.TECHNICAL == failureType; }
}
