package com.github.leopoldpetit.pocmb.common;

import com.github.leopoldpetit.pocmb.events.InventoryReservedEvent;
import com.github.leopoldpetit.pocmb.events.OrderCreatedEvent;
import com.github.leopoldpetit.pocmb.events.PaymentProcessedEvent;
import org.apache.avro.specific.SpecificRecord;

public final class OrderEventValidator {

    private OrderEventValidator() {
    }

    public static void validate(SpecificRecord event) {
        switch (event) {
            case OrderCreatedEvent e -> {
                requireSequence(e.getSequence(), 1);
                requireOrderId(e.getOrderId());
                if (e.getAmountCents() <= 0) {
                    throw new InvalidOrderEventException("amountCents must be positive for " + e.getOrderId());
                }
                if (e.getItems() == null || e.getItems().isEmpty()) {
                    throw new InvalidOrderEventException("order without items: " + e.getOrderId());
                }
            }
            case PaymentProcessedEvent e -> {
                requireSequence(e.getSequence(), 2);
                requireOrderId(e.getOrderId());
            }
            case InventoryReservedEvent e -> {
                requireSequence(e.getSequence(), 3);
                requireOrderId(e.getOrderId());
            }
            default -> throw new InvalidOrderEventException("Unsupported event: " + OrderEvents.type(event));
        }
    }

    private static void requireOrderId(String orderId) {
        if (orderId == null || orderId.isBlank()) {
            throw new InvalidOrderEventException("orderId is blank");
        }
    }

    private static void requireSequence(int actual, int expected) {
        if (actual != expected) {
            throw new InvalidOrderEventException("sequence " + actual + " does not match event type, expected " + expected);
        }
    }
}
