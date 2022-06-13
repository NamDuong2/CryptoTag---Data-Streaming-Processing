package org.davidcampos.kafka.commons;

public class Commons {
    public final static String EXAMPLE_KAFKA_TOPIC = System.getenv("EXAMPLE_KAFKA_TOPIC") != null ?
            System.getenv("EXAMPLE_KAFKA_TOPIC") : "tweetStreaming";
    public final static String EXAMPLE_KAFKA_GROUP = System.getenv("EXAMPLE_KAFKA_TOPIC") != null ?
            System.getenv("EXAMPLE_KAFKA_TOPIC") : "my-group";
    public final static String EXAMPLE_KAFKA_SERVER = System.getenv("EXAMPLE_KAFKA_SERVER") != null ?
            System.getenv("EXAMPLE_KAFKA_SERVER") : "192.168.0.106:9092";
    public final static String EXAMPLE_ZOOKEEPER_SERVER = System.getenv("EXAMPLE_ZOOKEEPER_SERVER") != null ?
            System.getenv("EXAMPLE_ZOOKEEPER_SERVER") : "192.168.0.106:2181";
}
