package com.github.leopoldpetit.pocmb.producer.load;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import com.github.leopoldpetit.pocmb.events.InventoryReservedEvent;
import com.github.leopoldpetit.pocmb.events.InventoryStatus;
import com.github.leopoldpetit.pocmb.events.OrderCreatedEvent;
import com.github.leopoldpetit.pocmb.events.OrderItem;
import com.github.leopoldpetit.pocmb.events.PaymentProcessedEvent;
import com.github.leopoldpetit.pocmb.events.PaymentStatus;
import com.github.leopoldpetit.pocmb.producer.publisher.OrderEventPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PreDestroy;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.stereotype.Service;

/** Emits orders at a fixed rate; the three events of one order are sent sequentially to keep their order. */
@Service
public class LoadGenerator {

    private final OrderEventPublisher publisher;
    private final MeterRegistry registry;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicLong submitted = new AtomicLong();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private ScheduledFuture<?> ticker;
    private ScheduledFuture<?> stopper;
    private LoadRequest active;

    public LoadGenerator(OrderEventPublisher publisher, MeterRegistry registry) {
        this.publisher = publisher;
        this.registry = registry;
        registry.gauge("poc.producer.inflight", Tags.of("transport", publisher.transport()), inFlight);
    }

    public synchronized LoadStatus start(LoadRequest requested) {
        LoadRequest request = requested.resolved();
        cancelTimers();
        submitted.set(0);
        active = request;
        Semaphore permits = new Semaphore(request.maxInFlight());
        long periodNanos = TimeUnit.SECONDS.toNanos(1) / request.ratePerSecond();
        ticker = scheduler.scheduleAtFixedRate(() -> submit(request, permits), 0, periodNanos, TimeUnit.NANOSECONDS);
        if (request.durationSeconds() > 0) {
            stopper = scheduler.schedule(this::stop, request.durationSeconds(), TimeUnit.SECONDS);
        }
        return status();
    }

    public synchronized LoadStatus stop() {
        cancelTimers();
        active = null;
        return status();
    }

    public synchronized LoadStatus status() {
        return new LoadStatus(active != null, publisher.transport(),
                active == null ? 0 : active.ratePerSecond(), inFlight.get(), submitted.get());
    }

    @PreDestroy
    void shutdown() {
        stop();
        scheduler.shutdownNow();
        workers.shutdownNow();
    }

    private void cancelTimers() {
        if (ticker != null) {
            ticker.cancel(false);
        }
        if (stopper != null) {
            stopper.cancel(false);
        }
    }

    private void submit(LoadRequest request, Semaphore permits) {
        if (request.maxOrders() > 0 && submitted.get() >= request.maxOrders()) {
            stop();
            return;
        }
        if (!permits.tryAcquire()) {
            registry.counter("poc.producer.orders", "outcome", "dropped").increment();
            return;
        }
        submitted.incrementAndGet();
        workers.execute(() -> {
            try {
                runOrder(request);
            } finally {
                permits.release();
            }
        });
    }

    private void runOrder(LoadRequest request) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String orderId = "o-" + UUID.randomUUID();
        inFlight.incrementAndGet();
        try {
            if (random.nextDouble() < request.invalidRate()) {
                String invalidId = "invalid-" + orderId;
                publish(invalidId, created(invalidId, request, -1L));
                outcome("invalid_sent");
                return;
            }
            publish(orderId, created(orderId, request, random.nextLong(1_000, 100_000)));
            publish(orderId, payment(orderId, random.nextDouble() < request.paymentFailureRate()));
            publish(orderId, inventory(orderId, random.nextDouble() < request.outOfStockRate()));
            outcome("completed");
        } catch (RuntimeException e) {
            outcome("failed");
        } finally {
            inFlight.decrementAndGet();
        }
    }

    private void publish(String key, SpecificRecord event) {
        publisher.publish(key, event).join();
    }

    private void outcome(String name) {
        registry.counter("poc.producer.orders", "outcome", name).increment();
    }

    private OrderCreatedEvent created(String orderId, LoadRequest request, long amountCents) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<OrderItem> items = IntStream.range(0, request.itemsPerOrder())
                .mapToObj(i -> OrderItem.newBuilder()
                        .setProductId("p-" + random.nextInt(1, 1_001))
                        .setQuantity(random.nextInt(1, 6))
                        .build())
                .toList();
        long now = System.currentTimeMillis();
        return OrderCreatedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId)
                .setSequence(1)
                .setCustomerId("c-" + random.nextInt(1, 10_001))
                .setAmountCents(amountCents)
                .setItems(items)
                .setPayloadPadding("x".repeat(request.payloadPaddingBytes()))
                .setOccurredAt(now)
                .setProducedAt(now)
                .build();
    }

    private PaymentProcessedEvent payment(String orderId, boolean failed) {
        long now = System.currentTimeMillis();
        return PaymentProcessedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId)
                .setSequence(2)
                .setStatus(failed ? PaymentStatus.FAILED : PaymentStatus.SUCCESS)
                .setTransactionId("tx-" + UUID.randomUUID())
                .setOccurredAt(now)
                .setProducedAt(now)
                .build();
    }

    private InventoryReservedEvent inventory(String orderId, boolean outOfStock) {
        long now = System.currentTimeMillis();
        return InventoryReservedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setOrderId(orderId)
                .setSequence(3)
                .setStatus(outOfStock ? InventoryStatus.OUT_OF_STOCK : InventoryStatus.RESERVED)
                .setOccurredAt(now)
                .setProducedAt(now)
                .build();
    }
}
