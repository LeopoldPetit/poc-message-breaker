package com.github.leopoldpetit.pocmb.producer.publisher;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.github.leopoldpetit.pocmb.common.PocTopology;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("rabbitmq")
public class RabbitOrderEventPublisher extends AbstractOrderEventPublisher {

    private final RabbitTemplate rabbitTemplate;
    private final KafkaAvroSerializer serializer;

    public RabbitOrderEventPublisher(RabbitTemplate rabbitTemplate, KafkaAvroSerializer serializer,
                                     MeterRegistry registry) {
        super("rabbitmq", registry);
        this.rabbitTemplate = rabbitTemplate;
        this.serializer = serializer;
        this.rabbitTemplate.setReturnsCallback(returned -> onReturned());
    }

    @Override
    protected CompletableFuture<Void> send(String orderId, SpecificRecord event) {
        byte[] body = serializer.serialize(PocTopology.ORDERS_TOPIC, event);
        MessageProperties properties = new MessageProperties();
        properties.setContentType(PocTopology.CONTENT_TYPE_AVRO);
        properties.setMessageId(UUID.randomUUID().toString());
        CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());

        rabbitTemplate.send(PocTopology.FANOUT_EXCHANGE, orderId, new Message(body, properties), correlation);

        return correlation.getFuture().thenCompose(confirm -> confirm.isAck()
                ? CompletableFuture.<Void>completedFuture(null)
                : CompletableFuture.<Void>failedFuture(new IllegalStateException("nacked: " + confirm.getReason())));
    }
}
