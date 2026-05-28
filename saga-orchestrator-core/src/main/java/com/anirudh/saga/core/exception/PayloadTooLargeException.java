package com.anirudh.saga.core.exception;

/**
 * Thrown when a saga payload exceeds the configured maximum size (P2-061 AC-2).
 *
 * <p>Maps to HTTP 413 Payload Too Large.
 */
public class PayloadTooLargeException extends RuntimeException {

    private final long actualBytes;
    private final long maxBytes;

    public PayloadTooLargeException(long actualBytes, long maxBytes) {
        super("PAYLOAD_TOO_LARGE: saga payload " + actualBytes + " bytes exceeds limit "
                + maxBytes + " (saga.payload.max-bytes)");
        this.actualBytes = actualBytes;
        this.maxBytes = maxBytes;
    }

    public long getActualBytes() { return actualBytes; }
    public long getMaxBytes() { return maxBytes; }
}
