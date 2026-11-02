package com.github.leopoldpetit.pocmb.consumer.rabbitmq.replay;

public record ReplayResult(String from, long processed, long failed, long durationMs) {
}
