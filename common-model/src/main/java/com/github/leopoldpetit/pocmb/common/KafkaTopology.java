package com.github.leopoldpetit.pocmb.common;

import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

public final class KafkaTopology {

    private KafkaTopology() {
    }

    public static KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(PocTopology.ORDERS_TOPIC)
                        .partitions(PocTopology.PARTITIONS).replicas(1).build(),
                TopicBuilder.name(PocTopology.ORDERS_DLT_TOPIC)
                        .partitions(PocTopology.PARTITIONS).replicas(1).build());
    }
}
