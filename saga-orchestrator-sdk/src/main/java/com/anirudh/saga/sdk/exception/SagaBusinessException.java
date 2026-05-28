package com.anirudh.saga.sdk.exception;

/**
 * Throw from @SagaCommandHandler to signal a BUSINESS failure.
 * SDK catches this and returns SagaReply.businessFailure() automatically.
 *
 * Usage:
 *   throw new SagaBusinessException("OUT_OF_STOCK");
 *   // SDK auto-classifies → businessFailure, no catch block needed
 */
public class SagaBusinessException extends RuntimeException {
    public SagaBusinessException(String message) { super(message); }
    public SagaBusinessException(String message, Throwable cause) { super(message, cause); }
}
