package com.github.leopoldpetit.pocmb.producer.load;

import java.util.Objects;

public record LoadRequest(
        Integer ratePerSecond,
        Integer itemsPerOrder,
        Integer payloadPaddingBytes,
        Double invalidRate,
        Double paymentFailureRate,
        Double outOfStockRate,
        Integer durationSeconds,
        Integer maxInFlight) {

    public LoadRequest resolved() {
        LoadRequest r = new LoadRequest(
                Objects.requireNonNullElse(ratePerSecond, 10),
                Objects.requireNonNullElse(itemsPerOrder, 2),
                Objects.requireNonNullElse(payloadPaddingBytes, 0),
                Objects.requireNonNullElse(invalidRate, 0.0),
                Objects.requireNonNullElse(paymentFailureRate, 0.05),
                Objects.requireNonNullElse(outOfStockRate, 0.05),
                Objects.requireNonNullElse(durationSeconds, 0),
                Objects.requireNonNullElse(maxInFlight, 500));
        require(r.ratePerSecond >= 1 && r.ratePerSecond <= 2000, "ratePerSecond must be in [1, 2000]");
        require(r.itemsPerOrder >= 1 && r.itemsPerOrder <= 20, "itemsPerOrder must be in [1, 20]");
        require(r.payloadPaddingBytes >= 0 && r.payloadPaddingBytes <= 1_000_000, "payloadPaddingBytes must be in [0, 1000000]");
        require(r.invalidRate >= 0 && r.invalidRate <= 1, "invalidRate must be in [0, 1]");
        require(r.paymentFailureRate >= 0 && r.paymentFailureRate <= 1, "paymentFailureRate must be in [0, 1]");
        require(r.outOfStockRate >= 0 && r.outOfStockRate <= 1, "outOfStockRate must be in [0, 1]");
        require(r.durationSeconds >= 0, "durationSeconds must be >= 0 (0 = unlimited)");
        require(r.maxInFlight >= 1 && r.maxInFlight <= 10_000, "maxInFlight must be in [1, 10000]");
        return r;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
