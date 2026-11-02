package com.github.leopoldpetit.pocmb.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class OrderSequenceCheckerTest {

    @Test
    void acceptsStepsInOrderAndForgetsCompletedOrders() {
        OrderSequenceChecker checker = new OrderSequenceChecker();

        assertTrue(checker.accept("o-1", 1));
        assertTrue(checker.accept("o-1", 2));
        assertTrue(checker.accept("o-1", 3));
        assertEquals(0, checker.trackedOrders());
    }

    @Test
    void rejectsStepThatSkipsAPreviousOne() {
        OrderSequenceChecker checker = new OrderSequenceChecker();

        assertFalse(checker.accept("o-2", 2));
        assertTrue(checker.accept("o-2", 1));
        assertFalse(checker.accept("o-2", 3));
    }
}
