package com.github.leopoldpetit.pocmb.consumer.rabbitmq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerRabbitApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerRabbitApplication.class, args);
    }
}
