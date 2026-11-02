package com.github.leopoldpetit.pocmb.consumer.rabbitmq.listener;

import com.github.leopoldpetit.pocmb.common.PocTopology;
import com.github.leopoldpetit.pocmb.consumer.rabbitmq.processing.RetryingEventHandler;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Streams the full history of orders. The offset is stored server-side under a named consumer, so a
 * restart resumes where it stopped.
 */
@Component
@ConditionalOnProperty(name = "poc.rabbitmq.stream.enabled", havingValue = "true", matchIfMissing = true)
public class StreamOrderConsumer {

    private static final String CONSUMER_NAME = "poc-stream-consumer";

    private final Environment environment;
    private final RetryingEventHandler handler;
    private final KafkaAvroDeserializer deserializer;
    private final RabbitTemplate rabbitTemplate;
    private final Counter deadLettered;
    private Consumer consumer;

    public StreamOrderConsumer(Environment streamEnvironment,
                               @Qualifier("streamEventHandler") RetryingEventHandler handler,
                               KafkaAvroDeserializer avroDeserializer,
                               RabbitTemplate rabbitTemplate,
                               MeterRegistry registry) {
        this.environment = streamEnvironment;
        this.handler = handler;
        this.deserializer = avroDeserializer;
        this.rabbitTemplate = rabbitTemplate;
        this.deadLettered = registry.counter("poc.dlq.published", "transport", "rabbitmq-stream");
    }

    @EventListener(ApplicationReadyEvent.class)
    public synchronized void start() {
        consumer = environment.consumerBuilder()
                .stream(PocTopology.STREAM)
                .name(CONSUMER_NAME)
                .offset(OffsetSpecification.first())
                .messageHandler(this::onMessage)
                .build();
    }

    @PreDestroy
    public synchronized void stop() {
        if (consumer != null) {
            consumer.close();
        }
    }

    private void onMessage(MessageHandler.Context context, com.rabbitmq.stream.Message message) {
        byte[] body = message.getBodyAsBinary();
        SpecificRecord event = decode(body);
        if (event == null || !handler.handle(event)) {
            deadLettered.increment();
            rabbitTemplate.send(PocTopology.DLX_EXCHANGE, PocTopology.DLQ_ROUTING_KEY,
                    new org.springframework.amqp.core.Message(body, messageProperties()));
        }
        context.storeOffset();
    }

    private SpecificRecord decode(byte[] body) {
        try {
            return (SpecificRecord) deserializer.deserialize(PocTopology.ORDERS_TOPIC, body);
        } catch (SerializationException e) {
            return null;
        }
    }

    private static org.springframework.amqp.core.MessageProperties messageProperties() {
        org.springframework.amqp.core.MessageProperties properties = new org.springframework.amqp.core.MessageProperties();
        properties.setContentType(PocTopology.CONTENT_TYPE_AVRO);
        return properties;
    }
}
