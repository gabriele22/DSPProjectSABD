package config;

public class ConfigurationKafka {

    private static final String KAFKA_BROKER_0 = "localhost:9092";
    private static final String KAFKA_BROKER_1 = "localhost:9093";
    private static final String KAFKA_BROKER_2 = "localhost:9094";
    private static final String SEP = ",";

    public static final String ZOOKEEPER = "localhost:2181";
    public static final String BOOTSTRAP_SERVERS = KAFKA_BROKER_0;
    public static final String BROKER_SERVERS =
            KAFKA_BROKER_0 + SEP +
                    KAFKA_BROKER_1 + SEP +
                    KAFKA_BROKER_2;



    public static final String PRODUCER_ID = "producer";
    public static final String TOPIC = "comments";
}
