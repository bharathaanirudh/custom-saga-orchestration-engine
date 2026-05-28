package com.anirudh.saga.core.exception;

public class SagaAlreadyExistsException extends RuntimeException {
    public SagaAlreadyExistsException(String idempotencyKey) {
        super("Saga already exists for idempotency key: " + idempotencyKey);
    }
}
