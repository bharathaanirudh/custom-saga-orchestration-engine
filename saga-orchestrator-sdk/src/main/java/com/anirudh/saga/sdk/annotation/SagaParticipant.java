package com.anirudh.saga.sdk.annotation;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SagaParticipant {
    /**
     * Module name — optional override. If empty, reads from saga.participant.module in application.yml.
     * Derives topic: {module}-commands, groupId: {module}-group.
     */
    String module() default "";
}
