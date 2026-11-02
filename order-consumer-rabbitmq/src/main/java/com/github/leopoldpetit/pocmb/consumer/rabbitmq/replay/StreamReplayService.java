package com.github.leopoldpetit.pocmb.consumer.rabbitmq.replay;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import com.github.leopoldpetit.pocmb.common.OrderEventProcessor;
import com.github.leopoldpetit.pocmb.common.PocTopology;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/** Reads the stream from the first offset or a timestamp with an unnamed consumer (no offset is stored). */
@Service
public class StreamReplayService {

    private static final Duration IDLE_BEFORE_END = Duration.ofSeconds(2);
    private static final Duration MAX_DURATION = Duration.ofMinutes(5);

    private final Environment environment;
    private final OrderEventProcessor processor;
    private final KafkaAvroDeserializer deserializer;

    public StreamReplayService(Environment streamEnvironment,
                               @Qualifier("streamProcessor") OrderEventProcessor processor,
                               KafkaAvroDeserializer avroDeserializer) {
        this.environment = streamEnvironment;
        this.processor = processor;
        this.deserializer = avroDeserializer;
    }

    /** Stops once the stream has been idle for a short period, which is the end of the replayable history. */
    public ReplayResult replay(Long fromTimestampMs) {
        long started = System.currentTimeMillis();
        AtomicLong processed = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong lastMessageAt = new AtomicLong(started);

        OffsetSpecification offset = fromTimestampMs == null
                ? OffsetSpecification.first()
                : OffsetSpecification.timestamp(fromTimestampMs);
        Consumer consumer = environment.consumerBuilder()
                .stream(PocTopology.STREAM)
                .offset(offset)
                .messageHandler((context, message) -> {
                    lastMessageAt.set(System.currentTimeMillis());
                    try {
                        SpecificRecord event = (SpecificRecord) deserializer.deserialize(
                                PocTopology.ORDERS_TOPIC, message.getBodyAsBinary());
                        processor.process(event, OrderEventProcessor.Mode.REPLAY);
                        processed.incrementAndGet();
                    } catch (RuntimeException e) {
                        failed.incrementAndGet();
                    }
                })
                .build();
        try {
            awaitEnd(lastMessageAt, started);
        } finally {
            consumer.close();
        }
        String from = fromTimestampMs == null ? "earliest" : "timestamp:" + fromTimestampMs;
        return new ReplayResult(from, processed.get(), failed.get(), System.currentTimeMillis() - started);
    }

    private void awaitEnd(AtomicLong lastMessageAt, long started) {
        while (true) {
            long now = System.currentTimeMillis();
            if (now - lastMessageAt.get() >= IDLE_BEFORE_END.toMillis() || now - started >= MAX_DURATION.toMillis()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
