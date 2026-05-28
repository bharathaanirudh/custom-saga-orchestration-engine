package com.anirudh.saga.core.exception;

public class SagaDefinitionNotFoundException extends RuntimeException {
    public SagaDefinitionNotFoundException(String sagaType) {
        super("No saga definition found for type: " + sagaType);
    }
}
