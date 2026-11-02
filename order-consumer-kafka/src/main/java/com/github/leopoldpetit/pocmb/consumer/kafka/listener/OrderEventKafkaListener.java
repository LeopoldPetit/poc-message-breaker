package com.github.leopoldpetit.pocmb.consumer.kafka.listener;

import com.github.leopoldpetit.pocmb.common.OrderEventProcessor;
import com.github.leopoldpetit.pocmb.common.PocTopology;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderEventKafkaListener {

    private final OrderEventProcessor processor;

    public OrderEventKafkaListener(OrderEventProcessor kafkaOrderEventProcessor) {
        this.processor = kafkaOrderEventProcessor;
    }

    @KafkaListener(id = "orders-kafka-live", topics = PocTopology.ORDERS_TOPIC,
            groupId = "poc-kafka-consumer", concurrency = "3")
    public void onMessage(ConsumerRecord<String, SpecificRecord> record, Acknowledgment acknowledgment) {
        processor.process(record.value(), OrderEventProcessor.Mode.LIVE);
        acknowledgment.acknowledge();
    }
}
