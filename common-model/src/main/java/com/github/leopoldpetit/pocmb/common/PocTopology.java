package com.github.leopoldpetit.pocmb.common;

public final class PocTopology {

    public static final String ORDERS_TOPIC = "orders.events";
    public static final String ORDERS_DLT_TOPIC = "orders.events.DLT";
    public static final int PARTITIONS = 3;

    public static final String FANOUT_EXCHANGE = "orders.events.fanout";
    public static final String HASH_EXCHANGE = "orders.events.hash";
    public static final String DLX_EXCHANGE = "orders.dlx";
    public static final String STREAM = "orders.events.stream";
    public static final String DLQ = "orders.dlq";
    public static final String DLQ_ROUTING_KEY = "orders.dlq";
    public static final String QUEUE_0 = "orders.events.q.0";
    public static final String QUEUE_1 = "orders.events.q.1";
    public static final String QUEUE_2 = "orders.events.q.2";
    public static final int DELIVERY_LIMIT = 3;

    public static final String CONTENT_TYPE_AVRO = "application/avro";

    private PocTopology() {
    }
}
