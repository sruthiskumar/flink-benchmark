import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import model.AggregatableObservation;
import model.FlowObservation;
import model.SpeedObservation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.log4j.Logger;
import util.DeserializationUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

public class InitializeSetUp {
    private  Properties flinkProperty;
    private  Properties kafkaProperty;


    static Logger logger = Logger.getLogger(InitializeSetUp.class);
    public InitializeSetUp(Properties flinkProperties, Properties kafkaProperties) throws IOException {
        this.flinkProperty = flinkProperties;
        this.kafkaProperty = kafkaProperties;
    }

    /**
 * Ingests the flow and speed data from Kafka
 *
 */
    public DataStream<Tuple3<String, String, Long>> ingestStage(StreamExecutionEnvironment env) {
        FlinkKafkaConsumer<Tuple3<String, String, Long>> kafkaSource = new FlinkKafkaConsumer(
                Arrays.asList(kafkaProperty.getProperty("flow.topic"), kafkaProperty.getProperty("speed.topic")),
//                kafkaProperty.getProperty("flow.topic"),
                new DeserializationUtil(), kafkaProperty);

        if (kafkaProperty.contains("earliest")) {
            kafkaSource.setStartFromEarliest();
        } else {
            kafkaSource.setStartFromLatest();
        }

        DataStream<Tuple3<String, String, Long>> rawStream = env.addSource(kafkaSource,
                TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){}))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, String, Long>>() {
                    long currentMaxTimestamp;
                    public Watermark getCurrentWatermark() {
                        //Move 50 to max.out.of.orderness config
                        return new Watermark(currentMaxTimestamp - 50L);
                    }

                    public long extractTimestamp(Tuple3<String, String, Long> element, long previousElementTimestamp) {
                        currentMaxTimestamp = Math.max(element.f2, currentMaxTimestamp);
                        return element.f2;
                    }
                });
        return rawStream;
    }

/**
 * Parses the  flow streams
 */
    public DataStream<FlowObservation> parseFlowStreams(DataStream<Tuple3<String, String, Long>> rawStream)  {
        DataStream<FlowObservation> flowStream = rawStream
                .filter(raw -> raw.f1.contains("flow"))
                .map(new MapFunction<Tuple3<String, String, Long>, FlowObservation>() {
            @Override
            public FlowObservation map(Tuple3<String, String, Long> value) throws Exception {
                JsonObject jsonObject = new JsonParser().parse(value.f1).getAsJsonObject();
                String time = jsonObject.get("timestamp").getAsString().substring(0,
                        jsonObject.get("timestamp").getAsString().indexOf("."));
                Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
                FlowObservation flowObservation
                        = new FlowObservation(value.f0.substring(0, value.f0.lastIndexOf("/")),
                        value.f0.substring(value.f0.lastIndexOf("/") + 1),
                        timeStamp,
                        jsonObject.get("lat").getAsDouble(),
                        jsonObject.get("long").getAsDouble(),
                        jsonObject.get("flow").getAsInt(),
                        jsonObject.get("period").getAsInt(),
                        jsonObject.get("accuracy").getAsInt(),
                        jsonObject.get("num_lanes").getAsInt(),
                        value.f2,
                        Instant.now().toEpochMilli());
                return flowObservation;
            }
        });
        return flowStream;
    }

    /**
     * Parses the  Speed streams
     */
    public DataStream<SpeedObservation> parseSpeedStreams(DataStream<Tuple3<String, String, Long>> rawStream)  {
        DataStream<SpeedObservation> speedObservationDataStream = rawStream
                .filter(raw -> raw.f1.contains("speed"))
                .map(new MapFunction<Tuple3<String, String, Long>, SpeedObservation>() {
                    @Override
                    public SpeedObservation map(Tuple3<String, String, Long> value) throws Exception {
                        JsonObject jsonObject = new JsonParser().parse(value.f1).getAsJsonObject();
                        String time = jsonObject.get("timestamp").getAsString().substring(0,
                                jsonObject.get("timestamp").getAsString().indexOf("."));
                        Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
                        SpeedObservation speedObservation
                                = new SpeedObservation(value.f0.substring(0, value.f0.lastIndexOf("/")),
                                value.f0.substring(value.f0.lastIndexOf("/") + 1),
                                timeStamp,
                                jsonObject.get("lat").getAsDouble(),
                                jsonObject.get("long").getAsDouble(),
                                jsonObject.get("speed").getAsDouble(),
                                jsonObject.get("accuracy").getAsInt(),
                                jsonObject.get("num_lanes").getAsInt(),
                                value.f2,
                                Instant.now().toEpochMilli());
                        return speedObservation;
                    }
                });
        return speedObservationDataStream;
    }

    /**
     * Joins the flow and speed Streams
     */
    public DataStream<AggregatableObservation> joinStreams(DataStream<FlowObservation> flowObservationDataStream,
                                                           DataStream<SpeedObservation> speedObservationDataStream) {
        DataStream<AggregatableObservation> joinedDataStream = flowObservationDataStream
                .keyBy(new KeySelector<FlowObservation, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> getKey(FlowObservation value) throws Exception {
                        return Tuple3.of(value.measurementId, value.internalId, value.timestamp);
                    }
                }).join(speedObservationDataStream.keyBy(new KeySelector<SpeedObservation, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> getKey(SpeedObservation value) throws Exception {
                        return Tuple3.of(value.measurementId, value.internalId, value.timestamp);
                    }
                }))
                .where(new KeySelector<FlowObservation, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> getKey(FlowObservation value) throws Exception {
                        return Tuple3.of(value.measurementId, value.internalId, value.timestamp);
                    }
                }).equalTo(new KeySelector<SpeedObservation, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> getKey(SpeedObservation value) throws Exception {
                        return Tuple3.of(value.measurementId, value.internalId, value.timestamp);
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<FlowObservation, SpeedObservation, AggregatableObservation>() {
                    @Override
                    public AggregatableObservation join(FlowObservation first, SpeedObservation second) throws Exception {
                        return new AggregatableObservation(first, second);
                    }
                });

//
//
//                .intervalJoin(speedObservationDataStream
//                        .keyBy(new KeySelector<SpeedObservation, Tuple3<String, String, Long>>() {
//                    @Override
//                    public Tuple3<String, String, Long> getKey(SpeedObservation value) throws Exception {
//                        return Tuple3.of(value.measurementId, value.internalId, value.timestamp);
//                    }
//                }))
//                .between(Time.milliseconds(-1 * Long.getLong(flinkProperty.getProperty("publish.interval.millis"))),
//                        Time.milliseconds(Long.getLong(flinkProperty.getProperty("publish.interval.millis"))))
//                .process(new ProcessJoinFunction<FlowObservation, SpeedObservation, AggregatableObservation>() {
//                    @Override
//                    public void processElement(FlowObservation left, SpeedObservation right, Context ctx, Collector<AggregatableObservation> out) throws Exception {
//                        out.collect(new AggregatableObservation(left, right));
//                    }
//                });
        return joinedDataStream;
    }
}
