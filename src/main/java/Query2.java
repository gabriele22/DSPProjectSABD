import config.ConfigurationKafka;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Int;
import utils.Comment;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query2 {

    private static final int NUM_CONSUMERS = 3;

    private static Properties props;
    private static int topN;



    private static void  initialize() {
        //set kafka properties
        props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConfigurationKafka.BOOTSTRAP_SERVERS);

        props.setProperty("zookeeper.connect",
                ConfigurationKafka.ZOOKEEPER);

        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                ConfigurationKafka.CONSUMER_GROUP_ID);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        topN =3;


    }



    public static void main(String[] args){
        initialize();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //set event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>(ConfigurationKafka.TOPIC, new JSONKeyValueDeserializationSchema(true), props);

        kafkaSource.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ObjectNode>() {
            long timestampStreamCompliant=0;
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(ObjectNode jsonNodes, long l) {
                return new Watermark(l);
            }

            @Override
            public long extractTimestamp(ObjectNode jsonNodes, long l) {
                long timestamp;
                try {
                    timestamp = jsonNodes.get("value").get("createDate").asLong();

                    timestampStreamCompliant = timestamp;
/*                    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
                    System.out.println("TIMESTAMP COMPLIANT: " +new Date(timestampStreamCompliant).toString());*/
                } catch (Exception e){
                    return timestampStreamCompliant; // RETURN PREVIOUS TIMESTAMP COMPLIANT
                }

                return timestampStreamCompliant;
            }
        });



        DataStream<Tuple2<String,Integer>> numCommentOnTwoHour = env
                .addSource(kafkaSource).setParallelism(NUM_CONSUMERS)
                .filter(new FindCorruptComment()).setParallelism(3)
                .filter(x-> x.get("value").get("depth").asInt()==1).setParallelism(3)
                .map(new GetCommentDepth())
/*
                .map(new SetKeyAndDepth()).setParallelism(3)

                .keyBy(0)
                .timeWindow(Time.hours(2))
                .sum(1).setParallelism(3)
                .map(x-> x.f1);
*/

                .timeWindowAll(Time.hours(2))
                .process(new CountDirectCommentAndAssignStart());

        SingleOutputStreamOperator<Tuple2<String, List<Tuple2<String,Integer>>>> numComments24Hours = numCommentOnTwoHour
                .timeWindowAll(Time.hours(24))
                .process(new PrintAllHour());

        SingleOutputStreamOperator<Tuple2<String, List<Tuple2<String,Integer>>>> numComments7Days = numCommentOnTwoHour
                .timeWindowAll(Time.days(7))
                .process(new PrintAllHour());



        numComments7Days.print();
        try {
            env.execute("Query 2");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    private static class FindCorruptComment implements FilterFunction<ObjectNode> {
        @Override
        public boolean filter(ObjectNode jsonNodes) throws Exception {
            Comment comment = null;
            //if all field of comment is not present it means that comment is corrupt
            try {
                comment = new Comment(jsonNodes.get("value").get("approveDate").asText(), jsonNodes.get("value").get("articleId").asText(),
                        jsonNodes.get("value").get("articleWordCount").asText(), jsonNodes.get("value").get("commentID").asText(), jsonNodes.get("value").get("commentType").asText(),
                        jsonNodes.get("value").get("createDate").asText(), jsonNodes.get("value").get("depth").asText(),
                        jsonNodes.get("value").get("editorsSelection").asText(), jsonNodes.get("value").get("inReplyTo").asText(),
                        jsonNodes.get("value").get("parentUserDisplayName").asText(), jsonNodes.get("value").get("recommendations").asText(),
                        jsonNodes.get("value").get("sectionName").asText(), jsonNodes.get("value").get("userDisplayName").asText(),
                        jsonNodes.get("value").get("userID").asText(), jsonNodes.get("value").get("userLocation").asText());
            } catch (Exception e) {
                return false;
            }

            //getter of Comment class cast string value in specific type or class
            //if at least one exception is launch, it means that comment is corrupt
            try {
                comment.getApproveDate();
                comment.getArticleId();
                comment.getArticleWordCount();
                comment.getCommentID();
                comment.getCommentType();
                comment.getCreateDate();
                comment.getDepth();
                comment.getEditorsSelection();
                comment.getInReplyTo();
                comment.getParentUserDisplayName();
                comment.getRecommendations();
                comment.getSectionName();
                comment.getUserDisplayName();
                comment.getUserID();
                comment.getUserLocation();
            } catch (Exception e) {
                return false;
            }

            return true;
        }
    }


    private static class GetCommentDepth implements MapFunction<ObjectNode, Integer> {
        @Override
        public Integer map(ObjectNode jsonNodes) throws Exception {
            String articleId = jsonNodes.get("value").get("articleId").asText();
            int depth = jsonNodes.get("value").get("depth").asInt();
            return depth;
        }
    }

/*    private static class SetKeyAndDepth implements MapFunction<ObjectNode, Tuple2<String,Integer>> {
        @Override
        public Tuple2<String,Integer> map(ObjectNode jsonNodes) throws Exception {

            int depth = jsonNodes.get("value").get("depth").asInt();
            return new Tuple2<>("ok",depth);
        }
    }*/

/*
    private static class CountOnTwoHour extends ProcessWindowFunction<Tuple2<Integer, Integer>, R, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> iterable, Collector<R> collector) throws Exception {

        }
    }*/

    private static class CountDirectCommentAndAssignStart extends ProcessAllWindowFunction<Integer, Tuple2<String,Integer>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Integer> iterable, Collector<Tuple2<String,Integer>> out) throws Exception {

            List<Integer> list = StreamSupport
                    .stream(iterable.spliterator(),false)
                    .collect(Collectors.toList());
            int count=0;
            for(Integer i: list){
                count = count +i;
            }
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            String initialTimestamp = new Date(context.window().getStart()).toString();

            out.collect(new Tuple2<>(initialTimestamp,count));



        }
    }

    private static class PrintAllHour extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, List<Tuple2<String,Integer>>>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, List<Tuple2<String,Integer>>>> out) throws Exception {
            List<Tuple2<String,Integer>> list = StreamSupport
                    .stream(iterable.spliterator(),false)
                    .collect(Collectors.toList());

            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            String initialTimestamp = new Date(context.window().getStart()).toString();

            out.collect(new Tuple2<>(initialTimestamp, list));

        }
    }

/*    private static class PrintAllHour extends ProcessAllWindowFunction< Integer, Tuple2<String, List<Integer>>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Integer> iterable, Collector<Tuple2<String, List<Integer>>> out) throws Exception {
            List<Integer> list = StreamSupport
                    .stream(iterable.spliterator(),false)
                    .collect(Collectors.toList());

            String initialTimestamp = new Date(context.window().getStart()).toString();

            out.collect(new Tuple2<>(initialTimestamp, list));

        }
    }*/

    private static class getArticleIdAndDepth implements MapFunction<ObjectNode, Tuple2<String,Integer>> {
        @Override
        public Tuple2<String,Integer> map(ObjectNode jsonNodes) throws Exception {
            String articleId = jsonNodes.get("value").get("articleId").asText();
            int depth = jsonNodes.get("value").get("depth").asInt();
            return new Tuple2<>(articleId,depth);
        }
    }
}



