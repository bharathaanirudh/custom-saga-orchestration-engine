package com.anirudh.saga.core.api;

import com.anirudh.saga.core.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class SagaExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(SagaExceptionHandler.class);

    @ExceptionHandler(SagaNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public SagaResponse<?> handleNotFound(SagaNotFoundException ex) {
        return SagaResponse.error("SAGA_NOT_FOUND", ex.getMessage());
    }

    @ExceptionHandler(SagaInvalidStateException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public SagaResponse<?> handleInvalidState(SagaInvalidStateException ex) {
        return SagaResponse.error("SAGA_INVALID_STATE", ex.getMessage());
    }

    @ExceptionHandler(SagaAlreadyExistsException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public SagaResponse<?> handleAlreadyExists(SagaAlreadyExistsException ex) {
        return SagaResponse.error("SAGA_ALREADY_EXISTS", ex.getMessage());
    }

    @ExceptionHandler(SagaDefinitionNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public SagaResponse<?> handleDefinitionNotFound(SagaDefinitionNotFoundException ex) {
        return SagaResponse.error("SAGA_DEFINITION_NOT_FOUND", ex.getMessage());
    }

    @ExceptionHandler(SagaLockException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public SagaResponse<?> handleLock(SagaLockException ex) {
        return SagaResponse.error("SAGA_LOCK_HELD", ex.getMessage());
    }

    @ExceptionHandler(IdempotencyPayloadMismatchException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public SagaResponse<?> handleIdempotencyMismatch(IdempotencyPayloadMismatchException ex) {
        return SagaResponse.error("IDEMPOTENCY_PAYLOAD_MISMATCH", ex.getMessage());
    }

    @ExceptionHandler(PayloadTooLargeException.class)
    @ResponseStatus(HttpStatus.PAYLOAD_TOO_LARGE)
    public SagaResponse<?> handlePayloadTooLarge(PayloadTooLargeException ex) {
        return SagaResponse.error("PAYLOAD_TOO_LARGE", ex.getMessage());
    }

    @ExceptionHandler(SagaExecutionException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public SagaResponse<?> handleExecution(SagaExecutionException ex) {
        log.error("Saga execution error: {}", ex.getMessage(), ex);
        return SagaResponse.error("SAGA_EXECUTION_ERROR", ex.getMessage());
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public SagaResponse<?> handleGeneral(Exception ex) {
        log.error("Unexpected error: {}", ex.getMessage(), ex);
        return SagaResponse.error("INTERNAL_ERROR", "An unexpected error occurred");
    }
}
