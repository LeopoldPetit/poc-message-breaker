package com.github.leopoldpetit.pocmb.producer.publisher;

import java.util.concurrent.CompletableFuture;

import com.github.leopoldpetit.pocmb.common.PocTopology;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Profile("kafka")
public class KafkaOrderEventPublisher extends AbstractOrderEventPublisher {

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;

    public KafkaOrderEventPublisher(KafkaTemplate<String, SpecificRecord> kafkaTemplate, MeterRegistry registry) {
        super("kafka", registry);
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    protected CompletableFuture<Void> send(String orderId, SpecificRecord event) {
        return kafkaTemplate.send(PocTopology.ORDERS_TOPIC, orderId, event).thenApply(result -> null);
    }
}
