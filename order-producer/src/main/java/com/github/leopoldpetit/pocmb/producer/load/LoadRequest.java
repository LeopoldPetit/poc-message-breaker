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
        Integer maxInFlight,
        Integer maxOrders) {

    public LoadRequest resolved() {
        LoadRequest r = new LoadRequest(
                Objects.requireNonNullElse(ratePerSecond, 10),
                Objects.requireNonNullElse(itemsPerOrder, 2),
                Objects.requireNonNullElse(payloadPaddingBytes, 0),
                Objects.requireNonNullElse(invalidRate, 0.0),
                Objects.requireNonNullElse(paymentFailureRate, 0.05),
                Objects.requireNonNullElse(outOfStockRate, 0.05),
                Objects.requireNonNullElse(durationSeconds, 0),
                Objects.requireNonNullElse(maxInFlight, 500),
                Objects.requireNonNullElse(maxOrders, 0));
        require(r.ratePerSecond >= 1 && r.ratePerSecond <= 20000, "ratePerSecond must be in [1, 20000]");
        require(r.itemsPerOrder >= 1 && r.itemsPerOrder <= 20, "itemsPerOrder must be in [1, 20]");
        require(r.payloadPaddingBytes >= 0 && r.payloadPaddingBytes <= 1_000_000, "payloadPaddingBytes must be in [0, 1000000]");
        require(r.invalidRate >= 0 && r.invalidRate <= 1, "invalidRate must be in [0, 1]");
        require(r.paymentFailureRate >= 0 && r.paymentFailureRate <= 1, "paymentFailureRate must be in [0, 1]");
        require(r.outOfStockRate >= 0 && r.outOfStockRate <= 1, "outOfStockRate must be in [0, 1]");
        require(r.durationSeconds >= 0, "durationSeconds must be >= 0 (0 = unlimited)");
        require(r.maxInFlight >= 1 && r.maxInFlight <= 10_000, "maxInFlight must be in [1, 10000]");
        require(r.maxOrders >= 0 && r.maxOrders <= 10_000_000, "maxOrders must be in [0, 10000000] (0 = unlimited)");
        return r;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
