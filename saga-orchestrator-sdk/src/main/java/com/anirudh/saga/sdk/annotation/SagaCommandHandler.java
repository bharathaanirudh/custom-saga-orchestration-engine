package com.anirudh.saga.sdk.annotation;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SagaCommandHandler {
    /**
     * Kafka topic to listen on. If empty, derived from @SagaParticipant(service) as "{service}-commands".
     */
    String topic() default "";

    /** Consumer group ID. Defaults to "{service}-service" if @SagaParticipant(service) is set. */
    String groupId() default "";

    /**
     * Specific action this method handles. If empty, method handles ALL actions for this topic.
     * Used for action-based routing: one method per action instead of a switch statement.
     */
    String action() default "";
}
