package com.anirudh.saga.core.exception;

public class SagaLockException extends RuntimeException {
    public SagaLockException(String message) {
        super(message);
    }
}
