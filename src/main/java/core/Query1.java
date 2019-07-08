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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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


    public Query1() {
    }

    public  static void startQuery1(DataStream<ObjectNode> commentCompliant, boolean latency){

        //if latency required, this part replicate transformation before tuples enter in a timing window
        if(latency){
            FlinkKafkaProducer<String> myProducerLatency = GetterFlinkKafkaProducer
                    .getConsumer(ConfigurationKafka.TOPIC_LATENCY_1_);
            commentCompliant
                    .map(new LatencyGetter()).setParallelism(3)
                    .countWindowAll(500)
                    .sum(2)
                    .map(new LatencyPrinter()).setParallelism(1)
                    .addSink(myProducerLatency);
        }


        //counts comments arrived in one hour for each article
        //map  0.articleId 1.counts
        DataStream<Tuple2<String, Integer>> cupleArticleIdOne = commentCompliant
                    .map(new OneSetter()).setParallelism(3);



        DataStream<Tuple2<String, Integer>>commentHour = cupleArticleIdOne
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .sum(1).setParallelism(3);
        //counts comments arrived in 24 hours for each article
        //(sum the previous result)
        DataStream<Tuple2<String, Integer>> comment24Hour = commentHour
                .keyBy(0)
                .timeWindow(Time.hours(24))
                .sum(1).setParallelism(3);

        //counts comments arrived in 7 days for each article
        //(sum the previous result)
        DataStream<Tuple2<String, Integer>> comment7Days = comment24Hour
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                .sum(1).setParallelism(3);


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


        /*Send results on KAFKA (real-time)*/
        rankingHour
                .map(new CreateString()).setParallelism(3)
                .addSink(myProducerHour).setParallelism(1);

        ranking24
                .map(new CreateString()).setParallelism(3)
                .addSink(myProducer24).setParallelism(1);

        ranking7Days
                .map(new CreateString()).setParallelism(3)
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



    //get latency for each tuple (milliseconds)
    private static class LatencyGetter implements MapFunction<ObjectNode, Tuple3<String,Integer,Long>> {
        @Override
        public Tuple3<String,Integer,Long> map(ObjectNode jsonNodes) throws Exception {
            String articleId = jsonNodes.get("value").get("articleId").asText();
            long entryTime = jsonNodes.get("entryTimeTuple").asLong();

            return new Tuple3<>(articleId, 1,System.nanoTime()-entryTime);
        }
    }

    //print mean of 500 tuple
    private static class LatencyPrinter implements MapFunction< Tuple3<String,Integer,Long>, String> {
        @Override
        public String map( Tuple3<String,Integer,Long> latencyOf500Tuples) throws Exception {
/*            Logger logger = LoggerFactory.getLogger(Query2.class);

            logger.info("MEAN LATENCY QUERY1 :" +latencyOf500Tuples.f2/500+ " ns" );*/
            String print = "MEAN LATENCY QUERY2 :" +latencyOf500Tuples.f2/500+ " ns";
            //System.out.printf("MEAN LATENCY QUERY1 : %d \n",latencyOf500Tuples.f2/500);
            return print;
        }
    }
}
