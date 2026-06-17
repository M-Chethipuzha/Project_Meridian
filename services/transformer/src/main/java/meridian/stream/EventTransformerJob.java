package meridian.stream;

import com.fasterxml.jackson.databind.JsonNode; import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint; import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema; import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource; import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;

public class EventTransformerJob {
    public static void main(String[] args) throws Exception {
        String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "localhost:19092");
        String sourceTopic = System.getenv().getOrDefault("KAFKA_SOURCE_TOPIC", "recentchanges");
        String sinkTopic = System.getenv().getOrDefault("KAFKA_SINK_TOPIC", "recentchanges-aggregated");
        String groupId = System.getenv().getOrDefault("KAFKA_GROUP", "meridian-transformer");
        int parallelism = Integer.parseInt(System.getenv().getOrDefault("FLINK_PARALLELISM", "2"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers).setTopics(sourceTopic).setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        DataStream<String> stream = env.fromSource(source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withIdleness(Duration.ofMinutes(1)), "recentchanges-source");

        ObjectMapper mapper = new ObjectMapper();
        TypeInformation<Tuple4<String, String, Long, String>> t4Info =
                TypeInformation.of(new TypeHint<Tuple4<String, String, Long, String>>() {});
        DataStream<Tuple4<String, String, Long, String>> parsed = stream
                .map(json -> {
                    JsonNode root = mapper.readTree(json);
                    String wiki = root.has("wiki") ? root.get("wiki").asText() : "unknown";
                    String type = root.has("type") ? root.get("type").asText() : "unknown";
                    long ts = root.has("timestamp") ? root.get("timestamp").asLong() : System.currentTimeMillis()/1000;
                    return Tuple4.of(wiki, type, ts, json);
                }).returns(t4Info)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple4<String, String, Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, t) -> event.f2 * 1000));

        TypeInformation<Tuple2<String, Long>> t2Info = TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {});
        TypeInformation<Tuple4<Long, Long, String, Long>> aggInfo = TypeInformation.of(new TypeHint<Tuple4<Long, Long, String, Long>>() {});

        DataStream<String> typeCounts = parsed
                .map(e -> Tuple2.of(e.f1, 1L)).returns(t2Info)
                .keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new CountAggregator(), aggInfo)
                .map(c -> String.format("{\"window\":{\"start\":%d,\"end\":%d},\"type\":\"%s\",\"count\":%d}", c.f0, c.f1, c.f2, c.f3));

        DataStream<String> wikiCounts = parsed
                .map(e -> Tuple2.of(e.f0, 1L)).returns(t2Info)
                .keyBy(e -> e.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new CountAggregator(), aggInfo)
                .map(c -> String.format("{\"window\":{\"start\":%d,\"end\":%d},\"wiki\":\"%s\",\"edits\":%d}", c.f0, c.f1, c.f2, c.f3));

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(sinkTopic).setValueSerializationSchema(new SimpleStringSchema()).build())
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

        typeCounts.union(wikiCounts).sinkTo(sink);
        env.execute("meridian-transformer");
    }

    public static class CountAggregator implements AggregateFunction<Tuple2<String, Long>, long[], Tuple4<Long, Long, String, Long>> {
        @Override public long[] createAccumulator() { return new long[]{0L}; }
        @Override public long[] add(Tuple2<String, Long> v, long[] acc) { acc[0]++; return acc; }
        @Override public Tuple4<Long, Long, String, Long> getResult(long[] acc) { return Tuple4.of(0L, 0L, "", acc[0]); }
        @Override public long[] merge(long[] a, long[] b) { return new long[]{a[0] + b[0]}; }
    }
}
