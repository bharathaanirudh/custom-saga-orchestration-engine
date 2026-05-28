package com.anirudh.saga.example.payment;

import com.anirudh.saga.sdk.contract.SagaHttpCommand;
import com.anirudh.saga.sdk.contract.SagaHttpReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/payment")
public class PaymentController {

    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    @PostMapping("/charge")
    public SagaHttpReply charge(@RequestBody SagaHttpCommand command) {
        String cardNumber = (String) command.payload().getOrDefault("cardNumber", "4111-xxxx");
        double amount = ((Number) command.payload().getOrDefault("amount", 0)).doubleValue();

        log.info("[sagaId={}] Payment charge amount={} card={}",
                command.sagaId(), amount, cardNumber);

        try {
            // Business failure example: card declined
            if ("DECLINED".equals(cardNumber)) {
                return SagaHttpReply.businessFailure(command.sagaId(), command.stepId(),
                        "CARD_DECLINED: " + cardNumber);
            }
            // Technical failure example: payment gateway down
            if (amount > 999999) {
                return SagaHttpReply.technicalFailure(command.sagaId(), command.stepId(),
                        "GATEWAY_LIMIT_EXCEEDED");
            }
            return SagaHttpReply.success(command.sagaId(), command.stepId(),
                    Map.of("transactionId", "TXN-" + command.sagaId().substring(0, 8)));
        } catch (Exception e) {
            return SagaHttpReply.technicalFailure(command.sagaId(), command.stepId(), e.getMessage());
        }
    }

    @PostMapping("/refund")
    public SagaHttpReply refund(@RequestBody SagaHttpCommand command) {
        log.info("[sagaId={}] Payment refund", command.sagaId());
        return SagaHttpReply.success(command.sagaId(), command.stepId(), "refunded");
    }
}
