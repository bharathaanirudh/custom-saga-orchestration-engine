package com.anirudh.saga.core.unit.engine;

import com.anirudh.saga.core.exception.SagaLockException;
import com.anirudh.saga.core.lock.SagaLock;
import com.anirudh.saga.core.lock.SagaLockManager;
import com.anirudh.saga.core.lock.SagaLockRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SagaLockManagerTest {

    @Mock SagaLockRepository lockRepository;

    SagaLockManager lockManager;
    Clock fixedClock;

    @BeforeEach
    void setUp() {
        fixedClock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);
        lockManager = new SagaLockManager(lockRepository, fixedClock);
    }

    @Test
    void acquire_newLock_saves() {
        lockManager.acquire("order", "ORD-1", "saga-1", "test-saga", Duration.ofMinutes(30));
        verify(lockRepository).save(any(SagaLock.class));
    }

    @Test
    void acquire_duplicateKey_andNotExpired_throwsLockException() {
        doThrow(new DuplicateKeyException("dup")).when(lockRepository).save(any());
        SagaLock existing = new SagaLock("order:ORD-1", "saga-other", "test",
                Instant.now(fixedClock), Instant.now(fixedClock).plusSeconds(3600));
        when(lockRepository.findById("order:ORD-1")).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> lockManager.acquire("order", "ORD-1", "saga-1", "test", Duration.ofMinutes(30)))
                .isInstanceOf(SagaLockException.class)
                .hasMessageContaining("held by saga");
    }

    @Test
    void acquire_duplicateKey_butExpired_replacesLock() {
        when(lockRepository.save(any()))
                .thenThrow(new DuplicateKeyException("dup"))
                .thenAnswer(inv -> inv.getArgument(0));
        SagaLock expired = new SagaLock("order:ORD-1", "saga-old", "test",
                Instant.now(fixedClock).minusSeconds(7200),
                Instant.now(fixedClock).minusSeconds(3600)); // expired 1hr ago
        when(lockRepository.findById("order:ORD-1")).thenReturn(Optional.of(expired));

        assertThatCode(() -> lockManager.acquire("order", "ORD-1", "saga-1", "test", Duration.ofMinutes(30)))
                .doesNotThrowAnyException();

        verify(lockRepository).deleteById("order:ORD-1");
        verify(lockRepository, times(2)).save(any()); // first fails, second succeeds
    }

    @Test
    void releaseAll_deletesBySagaId() {
        lockManager.releaseAll("saga-1");
        verify(lockRepository).deleteBySagaId("saga-1");
    }
}
