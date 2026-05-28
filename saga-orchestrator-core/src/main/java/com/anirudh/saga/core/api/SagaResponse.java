package com.anirudh.saga.core.api;

public record SagaResponse<T>(boolean success, T data, ErrorDetail error) {

    public static <T> SagaResponse<T> ok(T data) {
        return new SagaResponse<>(true, data, null);
    }

    public static <T> SagaResponse<T> error(String code, String message) {
        return new SagaResponse<>(false, null, new ErrorDetail(code, message));
    }

    public record ErrorDetail(String code, String message) {}
}
