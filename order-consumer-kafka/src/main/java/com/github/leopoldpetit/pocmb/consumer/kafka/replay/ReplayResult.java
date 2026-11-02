package com.github.leopoldpetit.pocmb.consumer.kafka.replay;

public record ReplayResult(String from, long processed, long failed, long durationMs) {
}
