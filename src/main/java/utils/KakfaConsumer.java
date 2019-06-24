package utils;

import config.ConfigurationKafka;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KakfaConsumer implements Runnable {

//    private final static String TOPIC = "a-simple-testing-topic";

    private final static String CONSUMER_GROUP_ID = "consumer";

    private Consumer<String, String> consumer;
    private int id;
    private String topic;

    public KakfaConsumer(int id, String topic){

        this.id = id;
        this.topic = topic;
        consumer = createConsumer();

        subscribeToTopic();

    }

    private Consumer<String, String> createConsumer() {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigurationKafka.BOOTSTRAP_SERVERS);

        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                CONSUMER_GROUP_ID);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        return new KafkaConsumer<>(props);
    }

    private void subscribeToTopic(){

        // To consume data, we first need to subscribe to the topics of interest
        consumer.subscribe(Collections.singletonList(this.topic));

    }

    public void listTopics() {

        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        for (String topicName : topics.keySet()) {

            if (topicName.startsWith("__"))
                continue;

            List<PartitionInfo> partitions = topics.get(topicName);
            for (PartitionInfo partition : partitions) {
                System.out.println("Topic: " +
                        topicName + "; Partition: " + partition.toString());
            }

        }

    }

    public void run() {

        boolean running = true;
        System.out.println("Consumer " + id + " running...");
        try {
            while (running) {
                Thread.sleep(1000);
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records)
                    System.out.println("[" + id + "] Consuming record:" +
                            " (key=" + record.key() + ", " +
                            "val=" + record.value() + ")");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }

}
