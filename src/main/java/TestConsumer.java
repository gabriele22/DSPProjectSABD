import config.ConfigurationKafka;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.Comment;
import utils.ranking.Rankable;
import utils.ranking.RankableObjectWithFields;
import utils.ranking.Rankings;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

public class TestConsumer {

    private static final int NUM_CONSUMERS = 3;

    private static Properties props;
    private static int topN;
    private static Rankings rankings;
    private static Rankings totalRankings;



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
        rankings = new Rankings(topN);
        totalRankings = new Rankings(topN);



    }

    public static void main (String args[]) {
/*

        KakfaConsumer consumer = new KakfaConsumer(-1, ConfigurationKafka.TOPIC);
        System.out.println("=== Listing topics === ");
        consumer.listTopics();
        System.out.println("=== === === === === === ");

        for (int i = 0; i < NUM_CONSUMERS; i++) {
            Thread c = new Thread(new KakfaConsumer(i, ConfigurationKafka.TOPIC));
            c.start();
        }
*/

        initialize();


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //set event time

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

/*

        FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>(ConfigurationKafka.TOPIC, new JSONKeyValueDeserializationSchema(true), props);

        kafkaSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ObjectNode>() {
            @Override
            public long extractAscendingTimestamp(ObjectNode jsonNodes) {
                Date timestamp = new  Date(jsonNodes.get("value").get("createDate").asLong());
                return timestamp.getTime();
                //return jsonNodes.get("key").asLong();
            }
        });

        //tuple3<String, String, Int>() 1.timestamp 2.articleId 3.counts
        DataStream<Tuple3<String,String, Integer>> comment = env
                .addSource(kafkaSource).setParallelism(3)//<---------------------------------------------------------------------
                .map(new SetOneToInstance())
                .keyBy(0)
                .timeWindow(Time.hours(1), Time.seconds(5))
                .sum(2);

*/


        FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>(ConfigurationKafka.TOPIC, new JSONKeyValueDeserializationSchema(true), props);

        kafkaSource.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ObjectNode>() {
            long timestampStreamCompliant=0;
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(ObjectNode jsonNodes, long l) {
                Watermark w = new Watermark(l);
               // System.out.println(w.toString());
                return w;
            }

            @Override
            public long extractTimestamp(ObjectNode jsonNodes, long l) {
                long timestamp;
                try {
                    timestamp = jsonNodes.get("value").get("createDate").asLong();
                    timestampStreamCompliant = timestamp;
                } catch (Exception npe){
                    System.err.println("CORRUPT RECORD");
                }

                return timestampStreamCompliant;
            }
        });


                //tuple3<String, String, Int>() 1.timestamp 2.articleId 3.counts
        DataStream < Tuple3 < String, String, Integer >> comment = env
                .addSource(kafkaSource).setParallelism(3)//<---------------------------------------------------------------------
                .filter(new FindCorruptComment())
/*                .filter(x->x.get("key")!=null && x.get("value")!=null)
                .filter(x->x.get("value").get("articleId")!=null && x.get("value").get("createDate")!=null)*/
                .map(new SetOneToInstance())
                .keyBy(1)
                .timeWindow(Time.hours(1), Time.minutes(15))
                .sum(2);
/*
                .flatMap(new GetRanking()).setParallelism(3)
                .flatMap(new GetFinalRanking()).setParallelism(1);
*/



        //.reduce((x,y)-> new Tuple2<>(x.f0,x.f1+y.f1));


        comment.print();



        try {
           env.execute("test");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static class SetOneToInstance implements MapFunction<ObjectNode, Tuple3<String,String,Integer>> {
        @Override
        public Tuple3<String, String,Integer> map(ObjectNode jsonNodes) throws Exception {
            String articleId = jsonNodes.get("value").get("articleId").asText();

/*            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
           String batchInitialTime = (new Date(jsonNodes.get("key").asLong()).toString());
            System.out.println("METADATA: "+ jsonNodes.get("metadata")+ "batchIntialTime: "+ batchInitialTime);*/
/*
            String createDate = jsonNodes.get("value").get("createDate").asText();
            System.out.println("METADATA: "+ jsonNodes.get("metadata")+ "batchIntialTime: "+createDate);*/

            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            String createDate = new Date(jsonNodes.get("value").get("createDate").asLong()).toString();

            return new Tuple3<>(createDate,articleId, 1);
        }

    }

    private static class FindCorruptComment implements org.apache.flink.api.common.functions.FilterFunction<ObjectNode> {
        @Override
        public boolean filter(ObjectNode jsonNodes) throws Exception {
            Comment comment=null;
            //if all field of comment is not present it means that comment is corrupt
            try {
                comment = new Comment(jsonNodes.get("value").get("approveDate").asText(), jsonNodes.get("value").get("articleId").asText(),
                        jsonNodes.get("value").get("articleWordCount").asText(), jsonNodes.get("value").get("commentID").asText(), jsonNodes.get("value").get("commentType").asText(),
                        jsonNodes.get("value").get("createDate").asText(), jsonNodes.get("value").get("depth").asText(),
                        jsonNodes.get("value").get("editorsSelection").asText(), jsonNodes.get("value").get("inReplyTo").asText(),
                        jsonNodes.get("value").get("parentUserDisplayName").asText(), jsonNodes.get("value").get("recommendations").asText(),
                        jsonNodes.get("value").get("sectionName").asText(), jsonNodes.get("value").get("userDisplayName").asText(),
                        jsonNodes.get("value").get("userID").asText(), jsonNodes.get("value").get("userLocation").asText());
            }catch (Exception e){
                System.err.println("CORRUPT COMMENT");
                return false;
            }

            //getter of Comment class cast string value in specific type or class
            //if at least one exception is launch, it means that comment is corrupt
            try{
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
            }catch (Exception e){
                System.err.println("CORRUPT COMMENT");
                return false;
            }

            return true;
        }
    }
/*
    private static class GetRanking implements FlatMapFunction<Tuple3<String, String, Integer>, Tuple3<String,String,Integer>> {
        @Override
        public void flatMap(Tuple3<String, String, Integer> stringStringIntegerTuple3, Collector<Tuple3<String, String, Integer>> out) throws Exception {
            Rankable rankable = RankableObjectWithFields.from(stringStringIntegerTuple3);
            rankings.updateWith(rankable);

            out.collect(rankings.getRankings());

        }
    }

    private static class GetFinalRanking implements FlatMapFunction<Tuple3<String, String, Integer>, R> {
        @Override
        public void flatMap(Tuple3<String, String, Integer> stringStringIntegerTuple3, Collector<R> collector) throws Exception {
            Rankings rankingsToBeMerged = (Rankings) stringStringIntegerTuple3.f0;
            totalRankings.updateWith(rankingsToBeMerged);
            totalRankings.pruneZeroCounts();
        }
    }*/
/*
    private static class SetOneToInstance implements MapFunction<String, Tuple2<String,Integer>> {
        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            Gson gson = new Gson();
            Comment comment = gson.fromJson(s, Comment.class);
            System.out.println(comment.toString());
            return new Tuple2<>(comment.getArticleId(), 1);
        }
    }
*/



}
