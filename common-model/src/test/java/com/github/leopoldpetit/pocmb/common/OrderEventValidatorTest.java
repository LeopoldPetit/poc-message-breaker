package com.github.leopoldpetit.pocmb.common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import com.github.leopoldpetit.pocmb.events.InventoryReservedEvent;
import com.github.leopoldpetit.pocmb.events.InventoryStatus;
import com.github.leopoldpetit.pocmb.events.OrderCreatedEvent;
import com.github.leopoldpetit.pocmb.events.OrderItem;
import org.junit.jupiter.api.Test;

class OrderEventValidatorTest {

    @Test
    void acceptsWellFormedCreatedEvent() {
        assertDoesNotThrow(() -> OrderEventValidator.validate(created("o-1", 1_000L)));
    }

    @Test
    void rejectsNonPositiveAmount() {
        assertThrows(InvalidOrderEventException.class,
                () -> OrderEventValidator.validate(created("o-2", -1L)));
    }

    @Test
    void rejectsSequenceThatDoesNotMatchEventType() {
        InventoryReservedEvent inventory = InventoryReservedEvent.newBuilder()
                .setEventId("e-1").setOrderId("o-3").setSequence(1)
                .setStatus(InventoryStatus.RESERVED).setOccurredAt(0L).setProducedAt(0L)
                .build();

        assertThrows(InvalidOrderEventException.class, () -> OrderEventValidator.validate(inventory));
    }

    private static OrderCreatedEvent created(String orderId, long amountCents) {
        return OrderCreatedEvent.newBuilder()
                .setEventId("e-" + orderId)
                .setOrderId(orderId)
                .setSequence(1)
                .setCustomerId("c-1")
                .setAmountCents(amountCents)
                .setItems(List.of(OrderItem.newBuilder().setProductId("p-1").setQuantity(2).build()))
                .setPayloadPadding("")
                .setOccurredAt(0L)
                .setProducedAt(0L)
                .build();
    }
}
