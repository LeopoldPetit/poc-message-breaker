package com.github.leopoldpetit.pocmb.common;

import com.github.leopoldpetit.pocmb.events.InventoryReservedEvent;
import com.github.leopoldpetit.pocmb.events.OrderCreatedEvent;
import com.github.leopoldpetit.pocmb.events.PaymentProcessedEvent;
import org.apache.avro.specific.SpecificRecord;

public final class OrderEvents {

    private OrderEvents() {
    }

    public static String orderId(SpecificRecord event) {
        return switch (event) {
            case OrderCreatedEvent e -> e.getOrderId();
            case PaymentProcessedEvent e -> e.getOrderId();
            case InventoryReservedEvent e -> e.getOrderId();
            default -> throw unknown(event);
        };
    }

    public static int sequence(SpecificRecord event) {
        return switch (event) {
            case OrderCreatedEvent e -> e.getSequence();
            case PaymentProcessedEvent e -> e.getSequence();
            case InventoryReservedEvent e -> e.getSequence();
            default -> throw unknown(event);
        };
    }

    public static long producedAt(SpecificRecord event) {
        return switch (event) {
            case OrderCreatedEvent e -> e.getProducedAt();
            case PaymentProcessedEvent e -> e.getProducedAt();
            case InventoryReservedEvent e -> e.getProducedAt();
            default -> throw unknown(event);
        };
    }

    public static String type(SpecificRecord event) {
        return event.getSchema().getName();
    }

    private static IllegalArgumentException unknown(SpecificRecord event) {
        return new IllegalArgumentException("Unsupported event type: " + type(event));
    }
}
