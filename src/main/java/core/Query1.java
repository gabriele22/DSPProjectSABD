package core;

import config.ConfigurationKafka;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
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

    private static final int NUM_CONSUMERS = 3;

    private static Properties props;
    private static final  int topN=3;


    public static void startQuery1( DataStream<ObjectNode> commentCompliant){

        //counts comments arrived in one hour for each article
        //(use world-count idea)
        DataStream<Tuple2<String, Integer>> commentHour = commentCompliant
                .map(new OneSetter()).setParallelism(2)
                //value 0.articleId 1.counts
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
                //.keyBy(0)
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



        //send results as String on Kafka
        FlinkKafkaProducer<String> myProducerHour = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_ONE_HOUR_);
        FlinkKafkaProducer<String> myProducer24 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_ONE_24_HOURS_);
        FlinkKafkaProducer<String> myProducer7 = GetterFlinkKafkaProducer.getConsumer(ConfigurationKafka.TOPIC_QUERY_ONE_7_DAYS_);


        rankingHour
                .map(new CreateString())
                .addSink(myProducerHour).setParallelism(1);

        ranking24
                .map(new CreateString())
                .addSink(myProducer24);

        ranking7Days
                .map(new CreateString())
                .addSink(myProducer7);


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
}
