package com.github.leopoldpetit.pocmb.common;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the last accepted step (1 to 3) per orderId; a step is accepted only if it is the next one.
 */
public final class OrderSequenceChecker {

    private static final int LAST_STEP = 3;

    private final ConcurrentHashMap<String, Integer> lastStep = new ConcurrentHashMap<>();

    public boolean accept(String orderId, int sequence) {
        boolean[] accepted = {false};
        lastStep.compute(orderId, (id, last) -> {
            int previous = last == null ? 0 : last;
            accepted[0] = sequence == previous + 1;
            if (!accepted[0]) {
                return last;
            }
            return sequence == LAST_STEP ? null : sequence;
        });
        return accepted[0];
    }

    public int trackedOrders() {
        return lastStep.size();
    }
}
