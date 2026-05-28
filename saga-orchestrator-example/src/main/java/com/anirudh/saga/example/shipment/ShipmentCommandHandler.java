package com.anirudh.saga.example.shipment;

import com.anirudh.saga.sdk.annotation.Idempotent;
import com.anirudh.saga.sdk.annotation.SagaCommandHandler;
import com.anirudh.saga.sdk.annotation.SagaParticipant;
import com.anirudh.saga.sdk.contract.SagaCommand;
import com.anirudh.saga.sdk.contract.SagaReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@SagaParticipant(module = "shipment")
public class ShipmentCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(ShipmentCommandHandler.class);

    @Idempotent
    @SagaCommandHandler(action = "SCHEDULE_SHIPMENT")
    public SagaReply schedule(SagaCommand command) {
        String orderId = (String) command.payload().getOrDefault("orderId", "unknown");
        log.info("[sagaId={}] Scheduling shipment for order={}", command.sagaId(), orderId);
        return SagaReply.success(command, Map.of("trackingId", "TRACK-" + orderId));
    }

    @Idempotent
    @SagaCommandHandler(action = "CANCEL_SHIPMENT")
    public SagaReply cancel(SagaCommand command) {
        log.info("[sagaId={}] Cancelling shipment", command.sagaId());
        return SagaReply.success(command, "cancelled");
    }
}
