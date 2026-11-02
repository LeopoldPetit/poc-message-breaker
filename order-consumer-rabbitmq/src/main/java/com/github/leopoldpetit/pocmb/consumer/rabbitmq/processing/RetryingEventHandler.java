package com.github.leopoldpetit.pocmb.consumer.rabbitmq.processing;

import com.github.leopoldpetit.pocmb.common.InvalidOrderEventException;
import com.github.leopoldpetit.pocmb.common.OrderEventProcessor;
import com.github.leopoldpetit.pocmb.common.TransientProcessingException;
import org.apache.avro.specific.SpecificRecord;

/**
 * Retries in the consumer thread instead of requeueing: a requeued message could be overtaken by a later
 * event of the same order. Returns false when the event must go to the dead-letter queue.
 */
public final class RetryingEventHandler {

    private final OrderEventProcessor processor;
    private final int maxAttempts;
    private final long backoffMs;

    public RetryingEventHandler(OrderEventProcessor processor, int maxAttempts, long backoffMs) {
        this.processor = processor;
        this.maxAttempts = maxAttempts;
        this.backoffMs = backoffMs;
    }

    public boolean handle(SpecificRecord event) {
        for (int attempt = 1; ; attempt++) {
            try {
                processor.process(event, OrderEventProcessor.Mode.LIVE);
                return true;
            } catch (InvalidOrderEventException e) {
                return false;
            } catch (TransientProcessingException e) {
                if (attempt >= maxAttempts || !pause()) {
                    return false;
                }
            }
        }
    }

    private boolean pause() {
        try {
            Thread.sleep(backoffMs);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
