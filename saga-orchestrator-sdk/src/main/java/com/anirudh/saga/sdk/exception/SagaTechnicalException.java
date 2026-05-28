package com.anirudh.saga.sdk.exception;

/**
 * Throw from @SagaCommandHandler to signal a TECHNICAL failure explicitly.
 * SDK catches this and returns SagaReply.technicalFailure() automatically.
 */
public class SagaTechnicalException extends RuntimeException {
    public SagaTechnicalException(String message) { super(message); }
    public SagaTechnicalException(String message, Throwable cause) { super(message, cause); }
}
