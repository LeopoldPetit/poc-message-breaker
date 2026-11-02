package com.github.leopoldpetit.pocmb.common;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import org.apache.avro.specific.SpecificRecord;

/** Simulates a slow consumer with a fixed delay and a random transient failure rate. */
public final class SimulatedBusinessProcessor {

    private final long delayMs;
    private final double failureRate;
    private final DoubleSupplier random;

    public SimulatedBusinessProcessor(long delayMs, double failureRate) {
        this(delayMs, failureRate, () -> ThreadLocalRandom.current().nextDouble());
    }

    SimulatedBusinessProcessor(long delayMs, double failureRate, DoubleSupplier random) {
        this.delayMs = delayMs;
        this.failureRate = failureRate;
        this.random = random;
    }

    public void process(SpecificRecord event) {
        pause();
        if (random.getAsDouble() < failureRate) {
            throw new TransientProcessingException("simulated transient failure on " + OrderEvents.type(event));
        }
    }

    private void pause() {
        if (delayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientProcessingException("interrupted while processing");
        }
    }
}
