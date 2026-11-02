package com.github.leopoldpetit.pocmb.producer.config;

import java.util.Map;

import com.github.leopoldpetit.pocmb.common.KafkaTopology;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class TransportConfig {

    @Bean
    @Profile("kafka")
    KafkaAdmin.NewTopics ordersKafkaTopics() {
        return KafkaTopology.topics();
    }

    @Bean
    @Profile("rabbitmq")
    KafkaAvroSerializer avroSerializer(@Value("${poc.schema-registry.url}") String schemaRegistryUrl) {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of("schema.registry.url", schemaRegistryUrl, "auto.register.schemas", true,
                "value.subject.name.strategy", RecordNameStrategy.class.getName()), false);
        return serializer;
    }
}
