package core;

import config.ConfigurationKafka;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import utils.GetterFlinkKafkaProducer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This class provides the total number of comments that are entered in two hours.
 * The number of comments is aggregated over three time windows: 24 hours, 7 days, 1 month
 */

public class Query2 {



    public static void startQuery2( DataStream<ObjectNode> commentCompliant, boolean latency){

        //if latency required, this part replicate transformation before tuples enter in a timing window
        if(latency){
            commentCompliant
                    .filter(x-> x.get("value").get("depth").asInt()==1).setParallelism(3)
                    .map(new LatencyGetterMilliSec()).setParallelism(3)
                    .countWindowAll(500)
                    .sum(2)
                    .map(new LatencyPrinter());
        }

        //map 0.hour 1.numCommentsOnTwoHours
        DataStream<Tuple2<String,Integer>> coupleHourOne = commentCompliant
                .filter(x-> x.get("value").get("depth").asInt()==1).setParallelism(3)
                .map(new SetterKeyAndOne()).setParallelism(3);

        //count number of DIRECT comments in two hours
        DataStream<Tuple2<String,Integer>> numCommentOnTwoHour= coupleHourOne
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(2)))
                .sum(1).setParallelism(3);


        //get ordered result of  24 hour window
        DataStream<Tuple2<String, List<Tuple2<String,Integer>>>> numComments24Hours = numCommentOnTwoHour
                .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
                .process(new SortResultsAndTakeInitialWindowTime());

        //count comments on two hours for 7 days and get ordered result
        DataStream<Tuple2<String, List<Tuple2<String,Integer>>>> numComments7Days= numCommentOnTwoHour
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(7),Time.days(-3)))
                .sum(1).setParallelism(3)
                .windowAll(TumblingEventTimeWindows.of(Time.days(7),Time.days(-3)))
                .process(new SortResultsAndTakeInitialWindowTime());


        //count comments on two hours for month and get ordered result
        DataStream<Tuple2<String, List<Tuple2<String,Integer>>>> numCommentsMonth= numCommentOnTwoHour
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(30),Time.days(12)))
                .sum(1).setParallelism(3)
                .windowAll(TumblingEventTimeWindows.of(Time.days(30),Time.days(12)))
                .process(new SortResultsAndTakeInitialWindowTime());



        //kafka producers
        FlinkKafkaProducer<String> myProducer24 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_TWO_24_HOUR_);
        FlinkKafkaProducer<String> myProducer7 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_TWO_7_DAYS_);
        FlinkKafkaProducer<String> myProducer30 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_TWO_30_DAYS_);

        /*Send results on KAFKA (real-time)*/
        numComments24Hours
                .map(new CreateString()).setParallelism(3)
                .addSink(myProducer24).setParallelism(1);
        numComments7Days.map(new CreateString()).setParallelism(3)
                .addSink(myProducer7).setParallelism(1);
        numCommentsMonth.map(new CreateString()).setParallelism(3)
                .addSink(myProducer30).setParallelism(1);


        /*write results on FILE*/
/*        numComments24Hours
                .map(new CreateString()).writeAsText("./results/query2-24-hours").setParallelism(1);
        numComments7Days
                .map(new CreateString()).writeAsText("./results/query2-7-days").setParallelism(1);
        numCommentsMonth
                .map(new CreateString()).writeAsText("./results/query2-30-days").setParallelism(1);*/


    }


    //set the key based on the value createDate and one for the next operation
    private static class SetterKeyAndOne implements MapFunction<ObjectNode, Tuple2<String,Integer>> {
        @Override
        public Tuple2<String,Integer> map(ObjectNode jsonNodes) throws Exception {

            long createDate = jsonNodes.get("value").get("createDate").asLong();
            LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(createDate), ZoneOffset.UTC.normalized());
            int h = localDateTime.getHour();
            String hourKey;

            if(h%2==0) {
                if (h <= 9) hourKey = "0" + h + ":00:00";
                else hourKey = h + ":00:00";
            }else {
                if (h <= 9) hourKey = "0" + (h - 1) + ":00:00";
                else hourKey = (h - 1) + ":00:00";
            }

            return new Tuple2<>(hourKey,1);
        }
    }

    //sort hours list and add initial timestamp of window
    private static class SortResultsAndTakeInitialWindowTime extends ProcessAllWindowFunction<Tuple2<String,Integer>, Tuple2<String, List<Tuple2<String,Integer>>>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple2<String,Integer>> iterable, Collector<Tuple2<String, List<Tuple2<String,Integer>>>> out) throws Exception {
            List<Tuple2<String,Integer>> countList = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .sorted(Comparator.comparing(x -> x.f0))
                    .collect(Collectors.toList());

            String initialTimestamp = new Date(context.window().getStart()).toString();

            out.collect(new Tuple2<>(initialTimestamp, countList));

        }
    }


    private static class CreateString implements MapFunction<Tuple2<String, List<Tuple2<String, Integer>>>, String> {
        @Override
        public String map(Tuple2<String, List<Tuple2<String, Integer>>> stringListTuple2) throws Exception {

            StringBuilder print = new StringBuilder();
            print.append(stringListTuple2.f0.toString());
            print.append(" --> ");
            for(Tuple2<String, Integer> tuple: stringListTuple2.f1){
                print.append("{").append(tuple.f0).append(", count = ").append(tuple.f1).append("} ");
            }

            return print.toString();
        }
    }


    //get latency for each tuple (milliseconds)
    private static class LatencyGetterMilliSec implements MapFunction<ObjectNode, Tuple3<String,Integer,Long>> {
        @Override
        public Tuple3<String,Integer,Long> map(ObjectNode jsonNodes) throws Exception {
            long createDate = jsonNodes.get("value").get("createDate").asLong();
            LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(createDate), ZoneOffset.UTC.normalized());
            int h = localDateTime.getHour();
            String hourKey;

            if(h%2==0) {
                if (h <= 9) hourKey = "0" + h + ":00:00";
                else hourKey = h + ":00:00";
            }else {
                if (h <= 9) hourKey = "0" + (h - 1) + ":00:00";
                else hourKey = (h - 1) + ":00:00";
            }
            long entryTime = jsonNodes.get("entryTimeTuple").asLong();



            return new Tuple3<>(hourKey,1,System.nanoTime()-entryTime);
        }
    }

    //print mean of 500 tuple
    private static class LatencyPrinter implements MapFunction<Tuple3<String,Integer,Long>, Void> {
        @Override
        public Void map(Tuple3<String,Integer,Long> latencyOf100Tuples) throws Exception {

            System.out.format("LATENCY QUERY2: %d %n",latencyOf100Tuples.f2/500);
            return null;
        }
    }

}



