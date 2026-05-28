package com.anirudh.saga.core.exception;

public class SagaNotFoundException extends RuntimeException {
    public SagaNotFoundException(String sagaId) {
        super("Saga not found: " + sagaId);
    }
}
