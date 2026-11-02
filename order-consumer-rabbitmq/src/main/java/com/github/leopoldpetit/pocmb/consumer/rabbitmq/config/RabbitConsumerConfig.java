package com.github.leopoldpetit.pocmb.consumer.rabbitmq.config;

import java.util.Map;

import com.github.leopoldpetit.pocmb.common.OrderEventProcessor;
import com.github.leopoldpetit.pocmb.common.RabbitTopology;
import com.github.leopoldpetit.pocmb.common.SimulatedBusinessProcessor;
import com.github.leopoldpetit.pocmb.consumer.rabbitmq.processing.RetryingEventHandler;
import com.rabbitmq.stream.Environment;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.core.Declarables;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConsumerConfig {

    @Bean
    Declarables ordersRabbitTopology() {
        return RabbitTopology.declarables();
    }

    @Bean
    KafkaAvroDeserializer avroDeserializer(@Value("${poc.schema-registry.url}") String schemaRegistryUrl) {
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(Map.of("schema.registry.url", schemaRegistryUrl, "specific.avro.reader", true), false);
        return deserializer;
    }

    @Bean
    OrderEventProcessor quorumProcessor(@Value("${poc.processing.delay-ms:0}") long delayMs,
                                        @Value("${poc.processing.failure-rate:0}") double failureRate,
                                        MeterRegistry registry) {
        return new OrderEventProcessor("rabbitmq", new SimulatedBusinessProcessor(delayMs, failureRate), registry);
    }

    @Bean
    OrderEventProcessor streamProcessor(@Value("${poc.processing.delay-ms:0}") long delayMs,
                                        @Value("${poc.processing.failure-rate:0}") double failureRate,
                                        MeterRegistry registry) {
        return new OrderEventProcessor("rabbitmq-stream", new SimulatedBusinessProcessor(delayMs, failureRate), registry);
    }

    @Bean
    RetryingEventHandler quorumEventHandler(@Qualifier("quorumProcessor") OrderEventProcessor processor,
                                            @Value("${poc.rabbitmq.retry.max-attempts:3}") int maxAttempts,
                                            @Value("${poc.rabbitmq.retry.backoff-ms:1000}") long backoffMs) {
        return new RetryingEventHandler(processor, maxAttempts, backoffMs);
    }

    @Bean
    RetryingEventHandler streamEventHandler(@Qualifier("streamProcessor") OrderEventProcessor processor,
                                            @Value("${poc.rabbitmq.retry.max-attempts:3}") int maxAttempts,
                                            @Value("${poc.rabbitmq.retry.backoff-ms:1000}") long backoffMs) {
        return new RetryingEventHandler(processor, maxAttempts, backoffMs);
    }

    @Bean(destroyMethod = "close")
    Environment streamEnvironment(@Value("${poc.rabbitmq.stream.host}") String host,
                                  @Value("${poc.rabbitmq.stream.port}") int port,
                                  @Value("${poc.rabbitmq.stream.username}") String username,
                                  @Value("${poc.rabbitmq.stream.password}") String password) {
        return Environment.builder()
                .host(host)
                .port(port)
                .username(username)
                .password(password)
                .build();
    }
}
