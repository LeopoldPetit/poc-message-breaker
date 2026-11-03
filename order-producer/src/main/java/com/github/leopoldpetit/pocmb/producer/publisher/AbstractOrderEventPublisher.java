package com.github.leopoldpetit.pocmb.producer.publisher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.github.leopoldpetit.pocmb.common.AuditLog;
import com.github.leopoldpetit.pocmb.common.OrderEvents;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.avro.specific.SpecificRecord;

public abstract class AbstractOrderEventPublisher implements OrderEventPublisher {

    private final String transport;
    private final MeterRegistry registry;
    private final AuditLog audit;

    protected AbstractOrderEventPublisher(String transport, MeterRegistry registry) {
        this.transport = transport;
        this.registry = registry;
        this.audit = AuditLog.fromEnv("producer-" + transport);
    }

    @Override
    public final CompletableFuture<Void> publish(String orderId, SpecificRecord event) {
        Tags tags = Tags.of("transport", transport, "type", OrderEvents.type(event));
        long start = System.nanoTime();
        return send(orderId, event).whenComplete((ignored, error) -> {
            registry.timer("poc.producer.confirm", tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            registry.counter(error == null ? "poc.producer.published" : "poc.producer.nacks", tags).increment();
            if (error == null && audit != null) {
                audit.record(System.currentTimeMillis() + "," + OrderEvents.eventId(event) + "," + orderId + ","
                        + OrderEvents.sequence(event) + "," + OrderEvents.type(event) + ",producer,confirmed");
            }
        });
    }

    @Override
    public final String transport() {
        return transport;
    }

    protected abstract CompletableFuture<Void> send(String orderId, SpecificRecord event);

    protected final void onReturned() {
        registry.counter("poc.producer.returns", "transport", transport).increment();
    }
}
