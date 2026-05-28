package com.anirudh.saga.core.exception;

/**
 * Thrown when a saga-start request reuses an existing idempotency key
 * but supplies a payload whose hash differs from the original (P2-061 AC-1).
 *
 * <p>Maps to HTTP 409 — explicit, not a silent overwrite.
 */
public class IdempotencyPayloadMismatchException extends RuntimeException {

    private final String idempotencyKey;
    private final String existingSagaId;

    public IdempotencyPayloadMismatchException(String idempotencyKey, String existingSagaId) {
        super("IDEMPOTENCY_PAYLOAD_MISMATCH: idempotencyKey=" + idempotencyKey
                + " was previously used for sagaId=" + existingSagaId
                + " with a different payload");
        this.idempotencyKey = idempotencyKey;
        this.existingSagaId = existingSagaId;
    }

    public String getIdempotencyKey() { return idempotencyKey; }
    public String getExistingSagaId() { return existingSagaId; }
}
