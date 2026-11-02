package com.github.leopoldpetit.pocmb.common;

import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;

public final class RabbitTopology {

    private RabbitTopology() {
    }

    /**
     * Fanout feeds the stream (full history) and an exchange-to-exchange binding to the consistent-hash exchange.
     * The routing key is the orderId, so all events of one order land in the same quorum queue.
     */
    public static Declarables declarables() {
        FanoutExchange fanout = new FanoutExchange(PocTopology.FANOUT_EXCHANGE, true, false);
        CustomExchange hash = new CustomExchange(PocTopology.HASH_EXCHANGE, "x-consistent-hash", true, false);
        DirectExchange dlx = new DirectExchange(PocTopology.DLX_EXCHANGE, true, false);

        Queue q0 = quorumQueue(PocTopology.QUEUE_0);
        Queue q1 = quorumQueue(PocTopology.QUEUE_1);
        Queue q2 = quorumQueue(PocTopology.QUEUE_2);
        Queue dlq = QueueBuilder.durable(PocTopology.DLQ).quorum().build();
        Queue stream = QueueBuilder.durable(PocTopology.STREAM).stream().build();

        return new Declarables(
                fanout, hash, dlx, q0, q1, q2, dlq, stream,
                new Binding(PocTopology.HASH_EXCHANGE, Binding.DestinationType.EXCHANGE,
                        PocTopology.FANOUT_EXCHANGE, "", Map.of()),
                BindingBuilder.bind(q0).to(hash).with("1").noargs(),
                BindingBuilder.bind(q1).to(hash).with("1").noargs(),
                BindingBuilder.bind(q2).to(hash).with("1").noargs(),
                BindingBuilder.bind(stream).to(fanout),
                BindingBuilder.bind(dlq).to(dlx).with(PocTopology.DLQ_ROUTING_KEY));
    }

    private static Queue quorumQueue(String name) {
        return QueueBuilder.durable(name)
                .quorum()
                .deadLetterExchange(PocTopology.DLX_EXCHANGE)
                .deadLetterRoutingKey(PocTopology.DLQ_ROUTING_KEY)
                .deliveryLimit(PocTopology.DELIVERY_LIMIT)
                .build();
    }
}
