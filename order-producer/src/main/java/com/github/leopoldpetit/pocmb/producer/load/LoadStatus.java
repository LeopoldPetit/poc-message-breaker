package com.github.leopoldpetit.pocmb.producer.load;

public record LoadStatus(boolean running, String transport, int ratePerSecond, int inFlight, long submitted) {
}
