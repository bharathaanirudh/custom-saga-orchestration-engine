package com.anirudh.saga.core.exception;

public class SagaInvalidStateException extends RuntimeException {
    public SagaInvalidStateException(String message) {
        super(message);
    }
}
