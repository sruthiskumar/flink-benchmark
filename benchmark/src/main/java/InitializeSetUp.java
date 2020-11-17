import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.Tuple0Serializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import util.DeserializationUtil;

import java.util.Arrays;
import java.util.Properties;

public class InitializeSetUp {
    private  Properties flinkProperty;
    private  Properties kafkaProperty;

    public InitializeSetUp(Properties flinkProperties, Properties kafkaProperties) {
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
                "ndwflow",
                new DeserializationUtil(), kafkaProperty);

        if (kafkaProperty.contains("earliest")) {
            kafkaSource.setStartFromEarliest();
        } else {
            kafkaSource.setStartFromLatest();
        }
        System.out.println("Hello World");

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
        rawStream.print();
        return rawStream;
    }
//
///**
// * Parses the speed and flow streams
// */
//    public DataStream<FlowObservation> filterSpeedStreams(DataStream<Tuple3<String, String, Long>> rawStream)  {
//        return rawStream.filter(val -> val.f1.contains("flow")).map()
//
//    }
}
