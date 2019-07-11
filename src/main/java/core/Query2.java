package core;

import config.ConfigurationKafka;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;
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

        //if LATENCY required, this part replicate all transformation adding latency count
        if(latency){
            FlinkKafkaProducer<String> myProducerLatency = GetterFlinkKafkaProducer
                    .getConsumer(ConfigurationKafka.TOPIC_LATENCY_2_);

            //count number of DIRECT comments in two hours and get max entry Time
            SingleOutputStreamOperator<Tuple3<String, Integer, Long>> numCommentOnTwoHourAndEntryTime = commentCompliant
                    .filter(x -> x.get("value").get("depth").asInt() == 1).setParallelism(2)
                    .map(new LatencyGetter()).setParallelism(2)
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.hours(2)))
                    .reduce(new SumAndGetMaxEntryTime());


            //get ordered result of  24 hour window
            DataStream<Tuple3<String, List<Tuple2<String,Integer>>,Long>> numComments24HoursAndLatency = numCommentOnTwoHourAndEntryTime
                    .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
                    .process(new SortResultsTakeInitialWindowTimeAndLatency());

            DataStream<Tuple3<String, List<Tuple2<String,Integer>>,Long>> numComments7DaysAndLatency= numCommentOnTwoHourAndEntryTime
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.days(7),Time.days(-3)))
                    .reduce(new SumAndGetMaxEntryTime())
                    .windowAll(TumblingEventTimeWindows.of(Time.days(7),Time.days(-3)))
                    .process(new SortResultsTakeInitialWindowTimeAndLatency());

            //count comments on two hours for month and get ordered result
            DataStream<Tuple3<String, List<Tuple2<String,Integer>>,Long>> numCommentsMonthAndLatency= numCommentOnTwoHourAndEntryTime
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.days(30),Time.days(12)))
                    .reduce(new SumAndGetMaxEntryTime())
                    .windowAll(TumblingEventTimeWindows.of(Time.days(30),Time.days(12)))
                    .process(new SortResultsTakeInitialWindowTimeAndLatency());


            //kafka producers
            FlinkKafkaProducer<String> myProducer24 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_TWO_24_HOUR_);
            FlinkKafkaProducer<String> myProducer7 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_TWO_7_DAYS_);
            FlinkKafkaProducer<String> myProducer30 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_TWO_30_DAYS_);

            /*Send results on KAFKA (real-time)*/
            numComments24HoursAndLatency
                    .map(new CreateStringWithLatency()).setParallelism(2)
                    .addSink(myProducer24).setParallelism(1);
            numComments7DaysAndLatency.map(new CreateStringWithLatency()).setParallelism(2)
                    .addSink(myProducer7).setParallelism(1);
            numCommentsMonthAndLatency.map(new CreateStringWithLatency()).setParallelism(2)
                    .addSink(myProducer30).setParallelism(1);



        }else {// WITHOUT LATENCY

            //map 0.hour 1.numCommentsOnTwoHours
            DataStream<Tuple2<String, Integer>> coupleHourOne = commentCompliant
                    .filter(x -> x.get("value").get("depth").asInt() == 1).setParallelism(2)
                    .map(new SetterKeyAndOne()).setParallelism(2);

            //count number of DIRECT comments in two hours
            DataStream<Tuple2<String, Integer>> numCommentOnTwoHour = coupleHourOne
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.hours(2)))
                    .sum(1).setParallelism(2);


            //get ordered result of  24 hour window
            DataStream<Tuple2<String, List<Tuple2<String, Integer>>>> numComments24Hours = numCommentOnTwoHour
                    .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
                    .process(new SortResultsAndTakeInitialWindowTime());

            //count comments on two hours for 7 days and get ordered result
            DataStream<Tuple2<String, List<Tuple2<String, Integer>>>> numComments7Days = numCommentOnTwoHour
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                    .sum(1).setParallelism(2)
                    .windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                    .process(new SortResultsAndTakeInitialWindowTime());


            //count comments on two hours for month and get ordered result
            DataStream<Tuple2<String, List<Tuple2<String, Integer>>>> numCommentsMonth = numCommentOnTwoHour
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.days(30), Time.days(12)))
                    .sum(1).setParallelism(2)
                    .windowAll(TumblingEventTimeWindows.of(Time.days(30), Time.days(12)))
                    .process(new SortResultsAndTakeInitialWindowTime());


            //kafka producers
            FlinkKafkaProducer<String> myProducer24 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_TWO_24_HOUR_);
            FlinkKafkaProducer<String> myProducer7 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_TWO_7_DAYS_);
            FlinkKafkaProducer<String> myProducer30 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_TWO_30_DAYS_);

            /*Send results on KAFKA (real-time)*/
            numComments24Hours
                    .map(new CreateString()).setParallelism(2)
                    .addSink(myProducer24).setParallelism(1);
            numComments7Days.map(new CreateString()).setParallelism(2)
                    .addSink(myProducer7).setParallelism(1);
            numCommentsMonth.map(new CreateString()).setParallelism(2)
                    .addSink(myProducer30).setParallelism(1);


            /*write results on FILE*/
/*        numComments24Hours
                .map(new CreateString()).writeAsText("./results/query2-24-hours").setParallelism(1);
        numComments7Days
                .map(new CreateString()).writeAsText("./results/query2-7-days").setParallelism(1);
        numCommentsMonth
                .map(new CreateString()).writeAsText("./results/query2-30-days").setParallelism(1);*/


        }
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


    /////////////////////////////// SAME METHOD with LATENCY ///////////////////////////////////////////

    //get latency for each tuple (milliseconds)
    private static class LatencyGetter implements MapFunction<ObjectNode, Tuple3<String,Integer,Long>> {
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



            return new Tuple3<>(hourKey,1, entryTime);
        }
    }

    private static class SumAndGetMaxEntryTime implements ReduceFunction<Tuple3<String, Integer, Long>> {
        @Override
        public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> tuple1, Tuple3<String, Integer, Long> tuple2) throws Exception {


            return new Tuple3<>(tuple1.f0,tuple1.f1+tuple2.f1,Math.max(tuple1.f2,tuple2.f2));
        }
    }

    private static class SortResultsTakeInitialWindowTimeAndLatency extends ProcessAllWindowFunction<Tuple3<String, Integer, Long>,  Tuple3<String, List<Tuple2<String,Integer>>, Long>, TimeWindow> {


        @Override
        public void process(Context context, Iterable<Tuple3<String, Integer, Long>> iterable, Collector<Tuple3<String, List<Tuple2<String,Integer>>, Long>> out) throws Exception {

            List<Long> entryTimeBigger = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .sorted((a, b) -> a.f2 > b.f2 ? -1 : a.f2 == b.f2 ? 0 : 1)
                    .limit(1)
                    .map(x-> x.f2)
                    .collect(Collectors.toList());

            long latency = System.currentTimeMillis() - entryTimeBigger.get(0);


            List<Tuple2<String,Integer>> countList = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .sorted(Comparator.comparing(x -> x.f0))
                    .map(x-> new Tuple2<>(x.f0,x.f1))
                    .collect(Collectors.toList());

            String initialTimestamp = new Date(context.window().getStart()).toString();

            out.collect(new Tuple3<>(initialTimestamp, countList, latency));
        }
    }

    private static class CreateStringWithLatency implements MapFunction<Tuple3<String, List<Tuple2<String, Integer>>, Long>, String> {
        @Override
        public String map(Tuple3<String, List<Tuple2<String, Integer>>, Long> tuple) throws Exception {

            StringBuilder print = new StringBuilder();
            print.append(tuple.f0.toString());
            print.append(" --> ");
            for(Tuple2<String, Integer> t: tuple.f1){
                print.append("{").append(t.f0).append(", count = ").append(t.f1).append("} ");
            }

            print.append("Latency = ").append(tuple.f2);
            return print.toString();
        }
    }
}




