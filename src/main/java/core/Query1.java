package core;

import config.ConfigurationKafka;
import org.apache.flink.api.common.functions.*;
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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
/**
 * This class provides the ranking of the 3 articles that received the most comments
 * on three different time windows: 1 hour, 24 hours, 7 days
 */

public class Query1 {


    private static final  int topN=3;



    public  static void startQuery1(DataStream<ObjectNode> commentCompliant, boolean latency){

        //if LATENCY required, this part replicate all transformation adding latency count
        if(latency){

            //counts comments arrived in one hour for each article and max entry Time
            DataStream<Tuple3<String, Integer,Long>>commentHourAndEntryTime =  commentCompliant
                    .map(new LatencyGetter()).setParallelism(2)
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.hours(1)))
                    .reduce(new SumAndGetMaxEntryTime()).setParallelism(2);

            //counts comments arrived in 24 hours for each article and max entry Time
            DataStream<Tuple3<String, Integer, Long>> comment24HourAndEntryTime= commentHourAndEntryTime
                    .keyBy(0)
                    .timeWindow(Time.hours(24))
                    .reduce(new SumAndGetMaxEntryTime()).setParallelism(2);

            //counts comments arrived in 7 days for each article and max entry Time
            DataStream<Tuple3<String, Integer,Long>> comment7DaysAndEntryTime = comment24HourAndEntryTime
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                    .reduce(new SumAndGetMaxEntryTime()).setParallelism(2);


            //get ranking in one hour and latency
            DataStream<Tuple3<Date, List<Tuple2<String, Integer>>, Long>> rankingHourAndEntryTime = commentHourAndEntryTime
                    .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                    .process(new RankingAndLatency()).setParallelism(1);

            //get ranking in 24 hours and latency
            DataStream<Tuple3<Date, List<Tuple2<String, Integer>>, Long>> ranking24AndEntryTime = comment24HourAndEntryTime
                    .timeWindowAll(Time.hours(24))
                    .process(new RankingAndLatency()).setParallelism(1);

            //get ranking in 7 days and latency
            DataStream<Tuple3<Date, List<Tuple2<String, Integer>>, Long>>ranking7DaysAndEntryTime = comment7DaysAndEntryTime
                    .windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                    .process(new RankingAndLatency()).setParallelism(1);



            //Kafka producers
            FlinkKafkaProducer<String> myProducerHour = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_ONE_HOUR_);
            FlinkKafkaProducer<String> myProducer24 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_ONE_24_HOURS_);
            FlinkKafkaProducer<String> myProducer7 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_ONE_7_DAYS_);


            /*Send results on KAFKA (real-time)*/
            rankingHourAndEntryTime
                    .map(new CreateStringWithLatency()).setParallelism(2)
                    .addSink(myProducerHour).setParallelism(1);

            ranking24AndEntryTime
                    .map(new CreateStringWithLatency()).setParallelism(2)
                    .addSink(myProducer24).setParallelism(1);

            ranking7DaysAndEntryTime
                    .map(new CreateStringWithLatency()).setParallelism(2)
                    .addSink(myProducer7).setParallelism(1);





        }else {// WITHOUT LATENCY


            //counts comments arrived in one hour for each article
            //map  0.articleId 1.counts
            DataStream<Tuple2<String, Integer>> cupleArticleIdOne = commentCompliant
                    .map(new OneSetter()).setParallelism(2);

            DataStream<Tuple2<String, Integer>> commentHour = cupleArticleIdOne
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.hours(1)))
                    .sum(1).setParallelism(2);

            //counts comments arrived in 24 hours for each article
            //(sum the previous result)
            DataStream<Tuple2<String, Integer>> comment24Hour = commentHour
                    .keyBy(0)
                    .timeWindow(Time.hours(24))
                    .sum(1).setParallelism(2);

            //counts comments arrived in 7 days for each article
            //(sum the previous result)
            DataStream<Tuple2<String, Integer>> comment7Days = comment24Hour
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                    .sum(1).setParallelism(2);

            //get ranking in one hour
            DataStream<Tuple2<Date, List<Tuple2<String, Integer>>>> rankingHour = commentHour
                    .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                    .process(new Ranking()).setParallelism(1);

            //get ranking in 24 hours
            DataStream<Tuple2<Date, List<Tuple2<String, Integer>>>> ranking24 = comment24Hour
                    .timeWindowAll(Time.hours(24))
                    .process(new Ranking()).setParallelism(1);

            //get ranking in 7 days
            DataStream<Tuple2<Date, List<Tuple2<String, Integer>>>> ranking7Days = comment7Days
                    .windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                    .process(new Ranking()).setParallelism(1);


            //Kafka producers
            FlinkKafkaProducer<String> myProducerHour = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_ONE_HOUR_);
            FlinkKafkaProducer<String> myProducer24 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_ONE_24_HOURS_);
            FlinkKafkaProducer<String> myProducer7 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_ONE_7_DAYS_);


            /*Send results on KAFKA (real-time)*/
            rankingHour
                    .map(new CreateString()).setParallelism(2)
                    .addSink(myProducerHour).setParallelism(1);

            ranking24
                    .map(new CreateString()).setParallelism(2)
                    .addSink(myProducer24).setParallelism(1);

            ranking7Days
                    .map(new CreateString()).setParallelism(2)
                    .addSink(myProducer7).setParallelism(1);




            /*write results on FILE*/
/*
        rankingHour
                .map(new CreateString())
                .writeAsText("./results/query1-one-hour-rank").setParallelism(1);

        ranking24
                .map(new CreateString())
                .writeAsText("./results/query1-24-hours-rank").setParallelism(1);
        ranking7Days
                .map(new CreateString())
                .writeAsText("./results/query1-7-days-rank").setParallelism(1);
*/

        }


    }

    //set One for the next operation
    private static class OneSetter implements MapFunction<ObjectNode, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String, Integer> map(ObjectNode jsonNodes) throws Exception {
            String articleId = jsonNodes.get("value").get("articleId").asText();
            return new Tuple2<>(articleId, 1);
        }

    }




    //sort the pairs <article, number of comments> to create the ranking
    // and take only the first topN elements,
    // also add initial timestamp of window
    private static class Ranking extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<Date, List<Tuple2<String, Integer>>>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<Date, List<Tuple2<String, Integer>>>> out) throws Exception {
            List<Tuple2<String, Integer>> ranking = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .sorted((a, b) -> a.f1 > b.f1 ? -1 : a.f1 == b.f1 ? 0 : 1)
                    .limit(topN)
                    .collect(Collectors.toList());

            Date initialTimestamp = new Date(context.window().getStart());

            out.collect(new Tuple2<>(initialTimestamp, ranking));

        }
    }


    private static class CreateString implements MapFunction<Tuple2<Date, List<Tuple2<String, Integer>>>, String> {
        @Override
        public String map(Tuple2<Date, List<Tuple2<String, Integer>>> ranking) throws Exception {
            StringBuilder print = new StringBuilder();
            print.append(ranking.f0.toString());
            print.append(" --> ");
            for(Tuple2<String, Integer> tuple: ranking.f1){
                print.append("{id = ").append(tuple.f0).append(", n.comm = ").append(tuple.f1).append("} ");
            }

            return print.toString();
        }
    }




    /////////////////////////////// SAME METHOD with LATENCY ///////////////////////////////////////////

    //get latency for each tuple (milliseconds)
    private static class LatencyGetter implements MapFunction<ObjectNode, Tuple3<String,Integer,Long>> {
        @Override
        public Tuple3<String,Integer,Long> map(ObjectNode jsonNodes) throws Exception {
            String articleId = jsonNodes.get("value").get("articleId").asText();
            long entryTime = jsonNodes.get("entryTimeTuple").asLong();

            return new Tuple3<>(articleId, 1, entryTime);
        }
    }

    private static class SumAndGetMaxEntryTime implements ReduceFunction<Tuple3<String, Integer, Long>> {
        @Override
        public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> tuple1, Tuple3<String, Integer, Long> tuple2) throws Exception {


            return new Tuple3<>(tuple1.f0,tuple1.f1+tuple2.f1,Math.max(tuple1.f2,tuple2.f2));
        }
    }


    private static class RankingAndLatency extends ProcessAllWindowFunction<Tuple3<String, Integer, Long>,  Tuple3<Date,List<Tuple2<String, Integer>>, Long>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple3<String, Integer, Long>> iterable, Collector< Tuple3<Date,List<Tuple2<String, Integer>>, Long>> collector) throws Exception {


            List<Long> entryTimeBigger = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .sorted((a, b) -> a.f2 > b.f2 ? -1 : a.f2 == b.f2 ? 0 : 1)
                    .limit(1)
                    .map(x-> x.f2)
                    .collect(Collectors.toList());



            List<Tuple2<String, Integer>> ranking = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .sorted((a, b) -> a.f1 > b.f1 ? -1 : a.f1 == b.f1 ? 0 : 1)
                    .map(x-> new Tuple2<>(x.f0,x.f1))
                    .limit(topN)
                    .collect(Collectors.toList());

            Date initialTimestamp = new Date(context.window().getStart());

            collector.collect(new Tuple3<>(initialTimestamp, ranking, entryTimeBigger.get(0)));

        }
    }

    private static class CreateStringWithLatency implements MapFunction<Tuple3<Date, List<Tuple2<String, Integer>>, Long>, String> {
        @Override
        public String map(Tuple3<Date, List<Tuple2<String, Integer>>, Long> ranking) throws Exception {

            StringBuilder print = new StringBuilder();
            print.append(ranking.f0.toString());
            print.append(" --> ");
            for(Tuple2<String, Integer> tuple: ranking.f1){
                print.append("{id = ").append(tuple.f0).append(", n.comm = ").append(tuple.f1).append("} ");
            }

            long latency = System.currentTimeMillis() - ranking.f2;

            print.append("Latency = ").append(latency);

            return print.toString();


        }
    }


}
