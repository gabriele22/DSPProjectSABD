package utils;


import config.ConfigurationKafka;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KakfaCommentProducer {



    private String topic;

    private Producer<String, Comment> producer;

    public KakfaCommentProducer(String topic) {

        this.topic = topic;
        producer = createProducer();

    }

    private static Producer<String, Comment> createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigurationKafka.BOOTSTRAP_SERVERS);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, ConfigurationKafka.PRODUCER_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, Comment>(props,new StringSerializer(),new KafkaJsonSerializer());
    }

    public void produce(String key, Comment comment) {


        try {

            final ProducerRecord<String, Comment> record = new ProducerRecord<>(topic, key, comment);

            RecordMetadata metadata = producer.send(record).get();

            // DEBUG
            System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                    record.key(), record.value(), metadata.partition(), metadata.offset());


        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
