package com.github.leopoldpetit.pocmb.producer.config;

import com.github.leopoldpetit.pocmb.common.RabbitTopology;
import org.springframework.amqp.core.Declarables;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("rabbitmq")
public class RabbitProducerConfig {

    @Bean
    Declarables ordersRabbitTopology() {
        return RabbitTopology.declarables();
    }
}
