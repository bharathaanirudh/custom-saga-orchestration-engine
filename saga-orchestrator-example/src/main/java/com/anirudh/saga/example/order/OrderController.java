package com.anirudh.saga.example.order;

import com.anirudh.saga.core.domain.SagaInstance;
import com.anirudh.saga.core.engine.SagaOrchestrator;
import com.anirudh.saga.sdk.contract.SagaStartRequest;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final SagaOrchestrator orchestrator;

    public OrderController(SagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping
    public Map<String, Object> placeOrder(@RequestBody Map<String, Object> request) {
        String orderId = (String) request.getOrDefault("orderId", UUID.randomUUID().toString());

        SagaStartRequest sagaRequest = new SagaStartRequest(
                "order-placement-saga",
                Map.of("orderId", orderId, "userId", request.get("userId")),
                "order-" + orderId
        );

        SagaInstance instance = orchestrator.start(sagaRequest);
        return Map.of("sagaId", instance.getSagaId(), "orderId", orderId, "status", instance.getStatus());
    }
}
