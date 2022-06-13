package org.davidcampos.kafka.consumer;


import cdorg.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONObject;
import org.davidcampos.kafka.commons.Commons;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



public class KafkaFlinkConsumerExample {
    private static final Logger logger = LogManager.getLogger(KafkaFlinkConsumerExample.class);
    public static final Integer HASHTAG_LIMIT = 6;
    public static final List<String> TagArray = new ArrayList<String>(Arrays.asList("#BTC", "#ETH", "#SOL", "#BNB", "#LUNA", "#NFT"));

    public static void main(final String... args) {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Properties
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.EXAMPLE_KAFKA_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Commons.EXAMPLE_KAFKA_GROUP);

        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>(Commons.EXAMPLE_KAFKA_TOPIC, new SimpleStringSchema(), props));

        // Parse JSON tweets, flatmap and emit keyed stream
        DataStream<Tuple2<String, Integer>> jsonTweets = messageStream.flatMap(new TweetFlatMapper())
                .keyBy(0);

        // Ordered topN list of most popular hashtags
        DataStream<LinkedHashMap<String, Integer>> ds = jsonTweets.timeWindowAll(Time.minutes(5), Time.seconds(10))
                .apply(new MostPopularTags());
        // Print to stdout
        ds.print();

        /*
        // Print final format stream
        DataStream<Tuple7<Long, Integer, Integer, Integer, Integer, Integer, Integer>> cryptoTagStream = ds.flatMap(new TupleNize());
        cryptoTagStream.print();
        */

        // Print final format stream
        DataStream<String> cryptoTagStream = ds.flatMap(new JsonNize());
        cryptoTagStream.print();


        // Write to Kafka
        cryptoTagStream.addSink(new FlinkKafkaProducer010("visualize-tweet", new SimpleStringSchema(), props));

        try {
            env.execute();
        } catch (Exception e) {
            logger.error("An error occurred.", e);
        }

    }

    // Convert datastream in LinkedHashMap -> JsonObject String
    private static class JsonNize implements FlatMapFunction<LinkedHashMap<String, Integer>, String> {
        @Override
        public void flatMap(LinkedHashMap<String, Integer> cryptotag, Collector<String> out) throws Exception {
            //Creating a JSONObject object
            JSONObject jsonObject = new JSONObject();
            //Inserting key-value pairs into the json object
            jsonObject.put("#BTC", cryptotag.get("#BTC"));
            jsonObject.put("#ETH", cryptotag.get("#ETH"));
            jsonObject.put("#BNB", cryptotag.get("#BNB"));
            jsonObject.put("#LUNA", cryptotag.get("#LUNA"));
            jsonObject.put("#NFT", cryptotag.get("#NFT"));
            jsonObject.put("#SOL", cryptotag.get("#SOL"));
            out.collect(jsonObject.toString());
        }
    }

    /*
    // Convert datastream in LinkedHashMap -> Tuple7
    private static class TupleNize implements FlatMapFunction<LinkedHashMap<String, Integer>, Tuple7<Long, Integer, Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public void flatMap(LinkedHashMap<String, Integer> cryptotag, Collector<Tuple7<Long, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
            out.collect(new Tuple7(System.currentTimeMillis(), cryptotag.get("#BTC"), cryptotag.get("#ETH"), cryptotag.get("#BNB"), cryptotag.get("#LUNA"), cryptotag.get("#NFT"), cryptotag.get("#SOL")));
        }
    }
    */

    // Convert datastream in String -> Tuple2<String, Integer>
    private static class TweetFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String tweet, Collector<Tuple2<String, Integer>> out) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            String tweetString = null;

            Pattern p = Pattern.compile("#\\w+");

            try {
                JsonNode jsonNode = mapper.readValue(tweet, JsonNode.class);
                tweetString = jsonNode.get("text").textValue();
            } catch (Exception e) {
                // That's ok
            }

            if (tweetString != null) {
                Matcher matcher = p.matcher(tweetString);

                while (matcher.find()) {
                    String cleanedHashtag = matcher.group(0).trim();
                    if (TagArray.contains(cleanedHashtag)) {
                        out.collect(new Tuple2(cleanedHashtag, 1));
                    }
                }
            }
        }
    }

    // Window functions
    public static class MostPopularTags implements AllWindowFunction<Tuple2<String, Integer>, LinkedHashMap<String, Integer>, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> tweets, Collector<LinkedHashMap<String, Integer>> collector) throws Exception {
            HashMap<String, Integer> hmap = new HashMap<String, Integer>();

            for (Tuple2<String, Integer> t: tweets) {
                int count = 0;
                if (hmap.containsKey(t.f0)) {
                    count = hmap.get(t.f0);
                }
                hmap.put(t.f0, count + t.f1);
            }

            Comparator<String> comparator = new ValueComparator(hmap);
            TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(comparator);

            sortedMap.putAll(hmap);

            LinkedHashMap<String, Integer> sortedTopN = sortedMap
                    .entrySet()
                    .stream()
                    .limit(HASHTAG_LIMIT)
                    .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

            collector.collect(sortedTopN);
        }
    }

    public static class ValueComparator implements Comparator<String> {
        HashMap<String, Integer> map = new HashMap<String, Integer>();

        public ValueComparator(HashMap<String, Integer> map){
            this.map.putAll(map);
        }

        @Override
        public int compare(String s1, String s2) {
            if (map.get(s1) >= map.get(s2)) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}



