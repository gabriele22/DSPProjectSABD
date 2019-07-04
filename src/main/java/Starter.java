import config.ConfigurationKafka;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import utils.FindCorruptComment;
import utils.GetterEventTImeAndWatermark;
import utils.InitializerKafka;

import java.util.Properties;

public class Starter {

    private static final int NUM_CONSUMERS = 3;


    public static void main (String[] args) {

        //initialize kafka properties
        Properties props = InitializerKafka.initialize();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //read json object from kafka
        FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>(ConfigurationKafka.TOPIC,
                new JSONKeyValueDeserializationSchema(true), props);

        //set event time to createDate value present in comments
        kafkaSource.assignTimestampsAndWatermarks(new GetterEventTImeAndWatermark());

        //get data from kafka, that will be passed to the query
        //before starting query, filter corrupt data
        DataStream<ObjectNode> commentCompliant = env
                .addSource(kafkaSource).setParallelism(NUM_CONSUMERS)
                .filter(new FindCorruptComment()).setParallelism(3);


        Query1.startQuery1(commentCompliant);
        Query2.startQuery2(commentCompliant);


        try {
            env.execute("SABD_project");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
