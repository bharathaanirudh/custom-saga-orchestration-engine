package com.anirudh.saga.example.inventory;

import com.anirudh.saga.sdk.annotation.Idempotent;
import com.anirudh.saga.sdk.annotation.SagaCommandHandler;
import com.anirudh.saga.sdk.annotation.SagaParticipant;
import com.anirudh.saga.sdk.contract.SagaCommand;
import com.anirudh.saga.sdk.contract.SagaReply;
import com.anirudh.saga.sdk.exception.SagaBusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@SagaParticipant(module = "inventory")
public class InventoryCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandHandler.class);

    private final Map<String, Integer> stock = new ConcurrentHashMap<>(Map.of(
            "PROD-001", 100, "PROD-002", 50, "PROD-003", 0
    ));

    @Idempotent
    @SagaCommandHandler(action = "RESERVE_INVENTORY")
    public SagaReply reserve(SagaCommand command) {
        String productId = (String) command.payload().getOrDefault("productId", "PROD-001");
        int qty = ((Number) command.payload().getOrDefault("quantity", 1)).intValue();

        log.info("[sagaId={}] Reserving {} units of {}", command.sagaId(), qty, productId);

        int available = stock.getOrDefault(productId, 0);
        if (available < qty) {
            // Business exception — SDK auto-classifies as businessFailure → compensate
            throw new SagaBusinessException("OUT_OF_STOCK: " + productId
                    + " available=" + available + " requested=" + qty);
        }

        stock.put(productId, available - qty);
        return SagaReply.success(command, Map.of("reservationId", "RES-" + command.sagaId().substring(0, 8)));
        // Any unexpected exception (NPE, ClassCast, etc.) → SDK auto-classifies as technicalFailure → suspend
    }

    @Idempotent
    @SagaCommandHandler(action = "RELEASE_INVENTORY")
    public SagaReply release(SagaCommand command) {
        String productId = (String) command.payload().getOrDefault("productId", "PROD-001");
        int qty = ((Number) command.payload().getOrDefault("quantity", 1)).intValue();

        log.info("[sagaId={}] Releasing {} units of {}", command.sagaId(), qty, productId);
        stock.merge(productId, qty, Integer::sum);
        return SagaReply.success(command, "released");
    }
}
