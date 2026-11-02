package com.github.leopoldpetit.pocmb.consumer.kafka.config;

import com.github.leopoldpetit.pocmb.common.InvalidOrderEventException;
import com.github.leopoldpetit.pocmb.common.KafkaTopology;
import com.github.leopoldpetit.pocmb.common.OrderEventProcessor;
import com.github.leopoldpetit.pocmb.common.PocTopology;
import com.github.leopoldpetit.pocmb.common.SimulatedBusinessProcessor;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    KafkaAdmin.NewTopics ordersKafkaTopics() {
        return KafkaTopology.topics();
    }

    @Bean
    OrderEventProcessor kafkaOrderEventProcessor(@Value("${poc.processing.delay-ms:0}") long delayMs,
                                                 @Value("${poc.processing.failure-rate:0}") double failureRate,
                                                 MeterRegistry registry) {
        return new OrderEventProcessor("kafka", new SimulatedBusinessProcessor(delayMs, failureRate), registry);
    }

    /** Three in-place retries (1 s apart) keep the partition order; then the record goes to the DLT. */
    @Bean
    CommonErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> template, MeterRegistry registry) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, error) -> new TopicPartition(PocTopology.ORDERS_DLT_TOPIC, record.partition()));
        Counter deadLettered = registry.counter("poc.dlq.published", "transport", "kafka");
        DefaultErrorHandler handler = new DefaultErrorHandler((record, error) -> {
            deadLettered.increment();
            recoverer.accept(record, error);
        }, new FixedBackOff(1_000L, 3L));
        handler.addNotRetryableExceptions(InvalidOrderEventException.class);
        return handler;
    }
}
