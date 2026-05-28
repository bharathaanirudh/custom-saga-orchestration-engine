package com.anirudh.saga.core.engine;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Component
public class IdempotencyGuard {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyGuard.class);

    private final SagaInstanceRepository repository;

    public IdempotencyGuard(SagaInstanceRepository repository) {
        this.repository = repository;
    }

    public String computeKey(String sagaType, Map<String, Object> payload) {
        try {
            String input = sagaType + ":" + new TreeMap<>(payload != null ? payload : Map.of());
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /**
     * Hash of just the payload (sagaType excluded) for collision detection (P2-061 AC-1).
     * Stored on {@link SagaInstance#getIdempotencyPayloadHash()}; compared on duplicate
     * idempotencyKey requests to detect "same key, different payload" attempts.
     */
    public String computePayloadHash(Map<String, Object> payload) {
        try {
            String input = new TreeMap<>(payload != null ? payload : Map.of()).toString();
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    public Optional<SagaInstance> findExisting(String idempotencyKey) {
        return repository.findByIdempotencyKey(idempotencyKey);
    }

    /**
     * Save with DuplicateKeyException handling.
     * If a concurrent request already created a saga with the same idempotencyKey,
     * MongoDB unique index throws DuplicateKeyException — we catch it and return existing.
     */
    public SagaInstance saveOrGetExisting(SagaInstance instance) {
        try {
            return repository.save(instance);
        } catch (DuplicateKeyException e) {
            log.info("[sagaId={}] DuplicateKeyException caught — concurrent request won the race, returning existing saga",
                    instance.getSagaId());
            return repository.findByIdempotencyKey(instance.getIdempotencyKey())
                    .orElseThrow(() -> new IllegalStateException(
                            "DuplicateKeyException but no saga found for key: " + instance.getIdempotencyKey()));
        }
    }
}
