package com.anirudh.saga.core.unit.engine;

import com.anirudh.saga.core.engine.IdempotencyGuard;
import com.anirudh.saga.core.repository.SagaInstanceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class IdempotencyGuardTest {

    @Mock SagaInstanceRepository repository;

    IdempotencyGuard guard;

    @BeforeEach
    void setUp() {
        guard = new IdempotencyGuard(repository);
    }

    @Test
    void computeKey_sameInput_returnsSameKey() {
        Map<String, Object> payload = Map.of("orderId", "ORD-001", "userId", "USR-123");

        String key1 = guard.computeKey("order-saga", payload);
        String key2 = guard.computeKey("order-saga", payload);

        assertThat(key1).isEqualTo(key2);
    }

    @Test
    void computeKey_differentSagaType_returnsDifferentKey() {
        Map<String, Object> payload = Map.of("orderId", "ORD-001");

        String key1 = guard.computeKey("order-saga", payload);
        String key2 = guard.computeKey("payment-saga", payload);

        assertThat(key1).isNotEqualTo(key2);
    }

    @Test
    void computeKey_differentPayload_returnsDifferentKey() {
        String key1 = guard.computeKey("order-saga", Map.of("orderId", "ORD-001"));
        String key2 = guard.computeKey("order-saga", Map.of("orderId", "ORD-002"));

        assertThat(key1).isNotEqualTo(key2);
    }

    @Test
    void computeKey_nullPayload_doesNotThrow() {
        String key = guard.computeKey("order-saga", null);
        assertThat(key).isNotBlank();
    }

    @Test
    void computeKey_emptyPayload_returnsDeterministicKey() {
        String key1 = guard.computeKey("order-saga", Map.of());
        String key2 = guard.computeKey("order-saga", Map.of());
        assertThat(key1).isEqualTo(key2);
    }

    @Test
    void computeKey_payloadOrderIndependent_returnsSameKey() {
        // TreeMap ensures key ordering — same result regardless of insertion order
        Map<String, Object> payload1 = Map.of("a", "1", "b", "2");
        Map<String, Object> payload2 = Map.of("b", "2", "a", "1");

        String key1 = guard.computeKey("saga", payload1);
        String key2 = guard.computeKey("saga", payload2);

        assertThat(key1).isEqualTo(key2);
    }

    @Test
    void computeKey_resultIsHexString() {
        String key = guard.computeKey("order-saga", Map.of("id", "1"));
        assertThat(key).matches("[0-9a-f]{64}"); // SHA-256 hex = 64 chars
    }
}
