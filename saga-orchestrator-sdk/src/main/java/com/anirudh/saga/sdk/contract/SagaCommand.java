package com.anirudh.saga.sdk.contract;

import java.util.Map;

public record SagaCommand(
        String sagaId,
        String stepId,
        String action,
        Map<String, Object> payload
) {}
