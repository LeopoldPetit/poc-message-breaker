package com.github.leopoldpetit.pocmb.common;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.avro.specific.SpecificRecord;

/** Validates, processes and measures one event for a given transport; shared by all consumers. */
public final class OrderEventProcessor {

    public enum Mode { LIVE, REPLAY }

    private final String transport;
    private final SimulatedBusinessProcessor business;
    private final OrderSequenceChecker sequenceChecker = new OrderSequenceChecker();
    private final MeterRegistry registry;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AuditLog audit;

    public OrderEventProcessor(String transport, SimulatedBusinessProcessor business, MeterRegistry registry) {
        this.transport = transport;
        this.business = business;
        this.registry = registry;
        this.audit = AuditLog.fromEnv("consumer-" + transport);
        registry.gauge("poc.consumer.inflight", Tags.of("transport", transport), inFlight);
    }

    public void process(SpecificRecord event, Mode mode) {
        String type = OrderEvents.type(event);
        Tags tags = Tags.of("transport", transport, "type", type, "mode", mode.name().toLowerCase(Locale.ROOT));
        long start = System.nanoTime();
        String status = "ok";
        inFlight.incrementAndGet();
        try {
            OrderEventValidator.validate(event);
            business.process(event);
            if (mode == Mode.LIVE) {
                recordSequence(event);
                recordLatency(event, tags);
            }
        } catch (InvalidOrderEventException e) {
            status = "invalid";
            throw e;
        } catch (TransientProcessingException e) {
            status = "retryable";
            throw e;
        } finally {
            if (audit != null && ("ok".equals(status) || "invalid".equals(status))) {
                audit.record(System.currentTimeMillis() + "," + OrderEvents.eventId(event) + ","
                        + OrderEvents.orderId(event) + "," + OrderEvents.sequence(event) + "," + type + ","
                        + mode.name().toLowerCase(Locale.ROOT) + "," + status);
            }
            inFlight.decrementAndGet();
            registry.counter("poc.consumer.processed", tags.and("status", status)).increment();
            registry.timer("poc.consumer.processing", tags).record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    private void recordSequence(SpecificRecord event) {
        if (!sequenceChecker.accept(OrderEvents.orderId(event), OrderEvents.sequence(event))) {
            registry.counter("poc.consumer.sequence.violations", "transport", transport).increment();
        }
    }

    private void recordLatency(SpecificRecord event, Tags tags) {
        long latencyMs = Math.max(0, System.currentTimeMillis() - OrderEvents.producedAt(event));
        registry.timer("poc.e2e.latency", tags).record(Duration.ofMillis(latencyMs));
    }
}
