package com.anirudh.saga.sdk.contract;

public record SagaHttpReply(
        String sagaId,
        String stepId,
        String status,
        FailureType failureType,
        Object data,
        String error
) {
    public static SagaHttpReply success(String sagaId, String stepId, Object data) {
        return new SagaHttpReply(sagaId, stepId, "SUCCESS", null, data, null);
    }

    public static SagaHttpReply businessFailure(String sagaId, String stepId, String reason) {
        return new SagaHttpReply(sagaId, stepId, "BUSINESS_FAILURE", FailureType.BUSINESS, null, reason);
    }

    public static SagaHttpReply technicalFailure(String sagaId, String stepId, String reason) {
        return new SagaHttpReply(sagaId, stepId, "TECHNICAL_FAILURE", FailureType.TECHNICAL, null, reason);
    }

    public boolean isSuccess() { return "SUCCESS".equals(status); }
    public boolean isBusinessFailure() { return FailureType.BUSINESS == failureType; }
    public boolean isTechnicalFailure() { return FailureType.TECHNICAL == failureType; }
}
