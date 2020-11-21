import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import model.FlowObservation;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.DeserializationUtil;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Properties;

public class InitializeSetUp {
    private  Properties flinkProperty;
    private  Properties kafkaProperty;

    Logger logger = LoggerFactory.getLogger(InitializeSetUp.class);

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
//                Arrays.asList(kafkaProperty.getProperty("flow.topic"), kafkaProperty.getProperty("speed.topic")),
                kafkaProperty.getProperty("flow.topic"),
                new DeserializationUtil(), kafkaProperty);

        if (kafkaProperty.contains("earliest")) {
            kafkaSource.setStartFromEarliest();
        } else {
            kafkaSource.setStartFromLatest();
        }
//        logger.info("Hello World");

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
        DataStream<FlowObservation> flowStream =
                rawStream.map(new MapFunction<Tuple3<String, String, Long>, FlowObservation>() {
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
}
