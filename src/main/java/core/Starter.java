package core;

import config.ConfigurationKafka;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.DeserializationAndAddEntryTime;
import utils.FindCorruptComment;
import utils.GetterEventTImeAndWatermark;
import utils.InitializerKafka;

import java.util.Properties;

public class Starter {

    private static final int NUM_CONSUMERS = 3;


    public static void main (String[] args) {
        boolean latencyTracking=false;

        if(args.length==0){
            System.err.println("ENTER DESIRED QUERY NAME : Query1 or Query2");
            System.err.println("AND ENTER 'true' TO ACTIVATE CUSTOM LATENCY TRACKING");
            return;
        }

        if(!(args[0].equalsIgnoreCase("Query1") || args[0].equalsIgnoreCase("Query2"))) {
            System.err.println("ENTER AT LEAST DESIRED QUERY NAME : Query1 or Query2");
            return;
        }

        try{
            latencyTracking = Boolean.parseBoolean(args[1]);
            if(latencyTracking)
                System.out.println("CUSTOM LATENCY TRACKING ACTIVE");
        }catch (Exception e){
            System.out.println("LATENCY TRACKING NOT ACTIVE");
            System.out.println("ENTER 'true' AFTER QUERY NAME TO ACTIVATE CUSTOM LATENCY TRACKING");

        }

        //initialize kafka properties
        Properties props = InitializerKafka.initialize();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //read json object from kafka
        DeserializationAndAddEntryTime deserializationAndAddEntryTime = new DeserializationAndAddEntryTime(false);
        FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>(ConfigurationKafka.TOPIC,
                deserializationAndAddEntryTime, props);




        //get data from kafka, that will be passed to the queries
        DataStream<ObjectNode> comment = env
                .addSource(kafkaSource).setParallelism(NUM_CONSUMERS);


        //before starting query, filter corrupt data
        //set event time to createDate value present in comments
        DataStream<ObjectNode> commentCompliant = comment
                .filter(new FindCorruptComment()).setParallelism(3)
                .assignTimestampsAndWatermarks(new GetterEventTImeAndWatermark()).setParallelism(3);




        if(args[0].equalsIgnoreCase("Query1")) {
            Query1.startQuery1(commentCompliant,latencyTracking);

            try {
                env.execute("SABD_project_query_1");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        if(args[0].equalsIgnoreCase("Query2")) {
            Query2.startQuery2(commentCompliant, latencyTracking);


            try {
                env.execute("SABD_project_query_2");
            } catch (Exception e) {
                e.printStackTrace();
            }

        }





    }
}
