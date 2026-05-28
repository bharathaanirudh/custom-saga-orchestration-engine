package com.anirudh.saga.core.exception;

public class SagaDefinitionInvalidException extends RuntimeException {
    public SagaDefinitionInvalidException(String message) {
        super(message);
    }
}
