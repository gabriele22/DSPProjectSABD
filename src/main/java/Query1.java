import config.ConfigurationKafka;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.Article;
import utils.Comment;
import utils.CommentEvent;
import utils.ranking.Rankable;
import utils.ranking.RankableObjectWithFields;
import utils.ranking.Rankings;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query1 {

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

    public static void main (String args[]) {

        //TODO rifare com 3 partizionei  e parallelism(3) sui Kafka source
        initialize();


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //set event time

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



        FlinkKafkaConsumer<ObjectNode> kafkaSource = new FlinkKafkaConsumer<>(ConfigurationKafka.TOPIC, new JSONKeyValueDeserializationSchema(true), props);

        kafkaSource.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ObjectNode>() {
            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(ObjectNode jsonNodes, long l) {
                Watermark w = new Watermark(l);
               // System.out.println(w.toString());
                return w;
            }

            @Override
            public long extractTimestamp(ObjectNode jsonNodes, long l) {
                long timestampStreamCompliant = 0;
                long timestamp;
                try {
                    timestamp = jsonNodes.get("value").get("createDate").asLong();

                    timestampStreamCompliant = timestamp;
/*                    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
                    System.out.println("TIMESTAMP COMPLIANT: " +new Date(timestampStreamCompliant).toString());*/
                } catch (Exception npe){
                    return timestampStreamCompliant; // RETURN PREVIOUS TIMESTAMP COMPLIANT
                }

                return timestampStreamCompliant;
            }
        });


                //tuple2<String, Int> 0.articleId 1.counts
        DataStream<Tuple2<String, Integer>> comment = env
                .addSource(kafkaSource).setParallelism(1)
                .filter(new FindCorruptComment()).setParallelism(3)
                .map(new SetOneToInstance()).setParallelism(3)
                .keyBy(0)
                .timeWindow(Time.hours(1))
                .sum(1).setParallelism(3);





/*
        DataStream<Tuple2<String, List<Article>>> ranking = comment
                .timeWindowAll(Time.hours(1))
                .aggregate(new UpdateRankings()).setParallelism(1)
                .map(new SetInitialTimestampOfSum());*/
/*
        DataStream<Tuple2<String, List<Article>>> ranking = comment
                .timeWindowAll(Time.hours(1))
                .aggregate(new UpdateRankings()).setParallelism(1);*/

        DataStream<Tuple2<String, List<Tuple2<String, Integer>>>> ranking = comment
                .timeWindowAll(Time.hours(1))
                .process(new MyProcessFunction()).setParallelism(1);



        ranking.print().setParallelism(1);
    //    ranking.addSink()





        try {
           env.execute("Query 1");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static class SetOneToInstance implements MapFunction<ObjectNode, Tuple2<String,Integer>> {
        @Override
        public Tuple2<String, Integer> map(ObjectNode jsonNodes) throws Exception {
            String articleId = jsonNodes.get("value").get("articleId").asText();
/*
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            Date createDate = new Date(jsonNodes.get("value").get("createDate").asLong());

            String createDate = jsonNodes.get("value").get("createDate").asText();
*/

            return new Tuple2<>(articleId, 1);
        }

    }

    private static class FindCorruptComment implements FilterFunction<ObjectNode> {
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
                return false;
            }

            return true;
        }
    }

    private static class GetIntermediateRank implements FlatMapFunction<Tuple3<String, String, Long>, Rankings> {
        private final Rankings iRankings;
        public GetIntermediateRank(int topN){
            iRankings= new Rankings(topN);
        }
        @Override
        public void flatMap(Tuple3<String, String, Long> tuple3, Collector<Rankings> out) throws Exception {

            Rankable rankable = RankableObjectWithFields.from(tuple3);
            iRankings.updateWith(rankable);
            out.collect(iRankings);
        }
    }

    private static class GetTotalRank implements FlatMapFunction<Rankings, Rankings> {
        private Rankings totalRankings;
        public GetTotalRank(int topN){
            totalRankings = new Rankings(topN);
        }
        @Override
        public void flatMap(Rankings rankings, Collector<Rankings> out) throws Exception {
            Rankings rankingsToBeMerged = rankings;
            totalRankings.updateWith(rankingsToBeMerged);
            totalRankings.pruneZeroCounts();
            out.collect(totalRankings);
        }
    }

    private static class SortFunction extends KeyedProcessFunction<String,CommentEvent, CommentEvent> {
        private ValueState<PriorityQueue<CommentEvent>> queueState = null;


        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<PriorityQueue<CommentEvent>> descriptor = new ValueStateDescriptor<>(
                    // state name
                    "sorted-events",
                    // type information of state
                    TypeInformation.of(new TypeHint<PriorityQueue<CommentEvent>>() {
                    }));
            queueState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(CommentEvent event, Context context, Collector<CommentEvent> out) throws Exception {
            TimerService timerService = context.timerService();

            if (context.timestamp() > timerService.currentWatermark()) {
                PriorityQueue<CommentEvent> queue = queueState.value();
                if (queue == null) {
                    queue = new PriorityQueue<>(topN);
                }
                queue.add(event);
                queueState.update(queue);
                timerService.registerEventTimeTimer(event.getTimestamp());
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<CommentEvent> out) throws Exception {
            PriorityQueue<CommentEvent> queue = queueState.value();
            Long watermark = context.timerService().currentWatermark();
            CommentEvent head = queue.peek();
            while (head != null && head.getTimestamp() <= watermark) {
                out.collect(head);
                queue.remove(head);
                head = queue.peek();
            }
        }

    }
/*
    private static class UpdateRankings implements AggregateFunction<Tuple3<String, String, Integer>, List<Article>, List<Article>> {
        @Override
        public List<Article> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<Article> add(Tuple3<String, String, Integer> tuple3, List<Article> acc) {
            acc.add(new Article(tuple3));
            return acc;
        }

        @Override
        public List<Article> getResult(List<Article> acc) {

            acc.sort((a, b) -> a.getNumComments() > b.getNumComments() ? -1 : a.getNumComments() == b.getNumComments() ? 0 : 1);

            //return getTopN(acc,topN);
            return acc;
        }

        @Override
        public List<Article> merge(List<Article> acc, List<Article> acc1) {
            List<Article> merge = new ArrayList<>();
            merge.addAll(acc);
            merge.addAll(acc1);
            //merge.sort((x, y) -> Integer.compare(x.getNumComments(), y.getNumComments()));
            merge.sort((a, b) -> a.getNumComments() > b.getNumComments() ? -1 : a.getNumComments() == b.getNumComments() ? 0 : 1);

            return getTopN(merge,topN);
        }

        private List<Article> getTopN(List<Article> acc,int topN){
            List<Article> articles = new ArrayList<>();
            int numElements = topN;
            if(acc.size()<topN){
                numElements=acc.size();
            }
            for(int i=0; i<numElements; i++){
                articles.add(acc.get(i));
            }
            return articles;
        }
    }*/


/*
    private static class UpdateRankings implements AggregateFunction<Tuple3<Date, String, Integer>, Tuple2<String,List<Article>>, Tuple2<String,List<Article>>> {
        @Override
        public Tuple2<String,List<Article>> createAccumulator() {
            String timestamp = "";
            List<Article> articles = new ArrayList<>();
            return new Tuple2<>(timestamp,articles);
        }

        @Override
        public Tuple2<String,List<Article>> add(Tuple3<Date, String, Integer> tuple3, Tuple2<String,List<Article>> acc) {
            acc.f1.add(new Article(tuple3));

            return acc;
        }

        @Override
        public Tuple2<String,List<Article>> getResult(Tuple2<String,List<Article>> acc) {

            acc.f1.sort((a, b) -> a.getNumComments() > b.getNumComments() ? -1 : a.getNumComments() == b.getNumComments() ? 0 : 1);
            List<Article> ranking = getTopN(acc.f1,topN);
            //acc.f0 = setInitialTimestamp(acc.f1);

            return new Tuple2<>(setInitialTimestamp(acc.f1), ranking);
        }

        @Override
        public Tuple2<String,List<Article>>  merge(Tuple2<String,List<Article>>  acc, Tuple2<String,List<Article>>  acc1) {
            List<Article> merge = new ArrayList<>();
            merge.addAll(acc.f1);
            merge.addAll(acc1.f1);
            //merge.sort((x, y) -> Integer.compare(x.getNumComments(), y.getNumComments()));
            merge.sort((a, b) -> a.getNumComments() > b.getNumComments() ? -1 : a.getNumComments() == b.getNumComments() ? 0 : 1);
            List<Article> ranking = getTopN(merge,topN);
            String initialTimestamp = setInitialTimestamp(merge);

            return new Tuple2<>(initialTimestamp,ranking);
        }

        private List<Article> getTopN(List<Article> acc,int topN){
            List<Article> articles = new ArrayList<>();
            int numElements = topN;
            if(acc.size()<topN){
                numElements=acc.size();
            }
            for(int i=0; i<numElements; i++){
                articles.add(acc.get(i));
            }
            return articles;
        }

        private String setInitialTimestamp (List<Article> list){
            Date initialStatisticTimestamp= list.get(0).getTimestamp();
            long initialSTlong = initialStatisticTimestamp.getTime();

            for (Article article : list) {
                long articleTSlong = article.getTimestamp().getTime();
                if (articleTSlong < initialSTlong) {
                    initialStatisticTimestamp = article.getTimestamp();
                }
            }

            return initialStatisticTimestamp.toString();
        }
    }
*/



    private static class MyProcessFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, List<Tuple2<String, Integer>>>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, List<Tuple2<String, Integer>>>> out) throws Exception {
            List<Tuple2<String, Integer>> commentsInWindow = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .sorted((a, b) -> a.f1 > b.f1 ? -1 : a.f1 == b.f1 ? 0 : 1)
                    .limit(topN)
                    .collect(Collectors.toList());

            String initialTimestamp = new Date(context.window().getStart()).toString();

            out.collect(new Tuple2<>(initialTimestamp, commentsInWindow));

        }
    }




/*
    private static class MyProcessFun extends ProcessWindowFunction<Tuple2<String, Integer>,Tuple2<String, List<Tuple2<String, Integer>>> , Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, List<Tuple2<String, Integer>>>> collector) throws Exception {

            String initialTimestamp = new Date(context.window().getStart()).toString();

            List<Tuple2<String, Integer>> commentsInWindow = StreamSupport
                    .stream(iterable.spliterator(), false)
                    .reduce((x,y)->new Tuple2<>(x.f0,x.f1+y.f1))
                    .sorted((a, b) -> a.f1 > b.f1 ? -1 : a.f1 == b.f1 ? 0 : 1)
                    .limit(topN)
                    .collect(Collectors.toList());



        }
    }
*/


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
