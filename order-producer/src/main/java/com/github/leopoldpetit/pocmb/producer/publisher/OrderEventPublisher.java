package com.github.leopoldpetit.pocmb.producer.publisher;

import java.util.concurrent.CompletableFuture;

import org.apache.avro.specific.SpecificRecord;

public interface OrderEventPublisher {

    /** Completes when the broker has confirmed the event, or completes exceptionally when it is refused. */
    CompletableFuture<Void> publish(String orderId, SpecificRecord event);

    String transport();
}
