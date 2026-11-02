package com.github.leopoldpetit.pocmb.consumer.rabbitmq.listener;

import java.io.IOException;

import com.github.leopoldpetit.pocmb.common.PocTopology;
import com.github.leopoldpetit.pocmb.consumer.rabbitmq.processing.RetryingEventHandler;
import com.rabbitmq.client.Channel;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** One listener per quorum queue, each with a single consumer, so each queue keeps its order. */
@Component
public class QuorumOrderListener {

    private final RetryingEventHandler handler;
    private final KafkaAvroDeserializer deserializer;
    private final Counter deadLettered;

    public QuorumOrderListener(@Qualifier("quorumEventHandler") RetryingEventHandler handler,
                               KafkaAvroDeserializer avroDeserializer,
                               MeterRegistry registry) {
        this.handler = handler;
        this.deserializer = avroDeserializer;
        this.deadLettered = registry.counter("poc.dlq.published", "transport", "rabbitmq");
    }

    @RabbitListener(id = "quorum-q0", queues = PocTopology.QUEUE_0, concurrency = "1")
    public void onQueue0(byte[] body, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        deliver(body, channel, tag);
    }

    @RabbitListener(id = "quorum-q1", queues = PocTopology.QUEUE_1, concurrency = "1")
    public void onQueue1(byte[] body, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        deliver(body, channel, tag);
    }

    @RabbitListener(id = "quorum-q2", queues = PocTopology.QUEUE_2, concurrency = "1")
    public void onQueue2(byte[] body, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        deliver(body, channel, tag);
    }

    private void deliver(byte[] body, Channel channel, long tag) throws IOException {
        SpecificRecord event = decode(body);
        if (event != null && handler.handle(event)) {
            channel.basicAck(tag, false);
            return;
        }
        deadLettered.increment();
        channel.basicNack(tag, false, false);
    }

    private SpecificRecord decode(byte[] body) {
        try {
            return (SpecificRecord) deserializer.deserialize(PocTopology.ORDERS_TOPIC, body);
        } catch (SerializationException e) {
            return null;
        }
    }
}
