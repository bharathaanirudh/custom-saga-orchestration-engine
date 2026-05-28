package com.anirudh.saga.sdk.annotation;

import java.lang.annotation.*;

/**
 * Marks a @SagaCommandHandler method as idempotent.
 * SDK stores processed (sagaId + stepId + action) in a dedup collection.
 * If duplicate command arrives, SDK returns the cached reply without calling the handler.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Idempotent {}
