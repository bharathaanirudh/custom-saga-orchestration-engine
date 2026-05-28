package com.anirudh.saga.sdk.contract;

import java.util.Map;

public record SagaStartRequest(
        String sagaType,
        Map<String, Object> payload,
        String idempotencyKey
) {}
