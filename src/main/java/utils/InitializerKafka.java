package utils;

import config.ConfigurationKafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class InitializerKafka {

    public static Properties  initialize() {
        //set kafka properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigurationKafka.BOOTSTRAP_SERVERS);

        props.setProperty("zookeeper.connect",
                ConfigurationKafka.ZOOKEEPER);

        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                ConfigurationKafka.CONSUMER_GROUP_ID);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        return props;


    }
}
