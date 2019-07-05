package config;

public class ConfigurationKafka {

/*    private static final String KAFKA_BROKER_0 = "localhost:9092";
    private static final String KAFKA_BROKER_1 = "localhost:9093";
    private static final String KAFKA_BROKER_2 = "localhost:9094";
    */
    private static final String KAFKA_BROKER_0 = "kafka0:9092";
    private static final String KAFKA_BROKER_1 = "kafka1:9093";
    private static final String KAFKA_BROKER_2 = "kafka2:9094";
    private static final String SEP = ",";

/*    public static final String ZOOKEEPER = "localhost:2181";*/
    public static final String ZOOKEEPER = "zookeeper:2181";
    //public static final String BOOTSTRAP_SERVERS = KAFKA_BROKER_0;
    public static final String BOOTSTRAP_SERVERS=
            KAFKA_BROKER_0 + SEP +
                    KAFKA_BROKER_1 + SEP +
                    KAFKA_BROKER_2;

    public static final String PRODUCER_ID = "producer";
    public static final String TOPIC = "comments";
    public final static String CONSUMER_GROUP_ID = "consumer";

    //QUERY RESULTS
    public static final String TOPIC_QUERY_ONE_HOUR_ = "query1-one-hour-rank";
    public static final String TOPIC_QUERY_ONE_24_HOURS_ = "query1-24-hours-rank";
    public static final String TOPIC_QUERY_ONE_7_DAYS_ =  "query1-7-days-rank";

    public static final String TOPIC_QUERY_TWO_24_HOUR_ = "query2-24-hours";
    public static final String TOPIC_QUERY_TWO_7_DAYS_ = "query2-7-days";
    public static final String TOPIC_QUERY_TWO_30_DAYS_ = "query2-30-days";



}
