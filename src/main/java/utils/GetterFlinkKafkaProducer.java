package utils;
import config.ConfigurationKafka;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class GetterFlinkKafkaProducer {

    public static FlinkKafkaProducer<String> getConsumer(String topicName){
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                ConfigurationKafka.BOOTSTRAP_SERVERS,            // broker list
                topicName,                                      // target topic
                new SimpleStringSchema());                      // serialization schema


        return myProducer;
    }
}
