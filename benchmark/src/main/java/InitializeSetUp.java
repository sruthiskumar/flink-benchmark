import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class InitializeSetUp {
    private static Properties kafkaProperty = loadKafkaProperties();
    private static Properties loadKafkaProperties() {
        Properties properties = new Properties();
        String file = "kafka.properties";
        InputStream inputStream = InitializeSetUp.class.getClassLoader().getResourceAsStream(file);
        if(inputStream != null){
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties;
    }

/**
 * Ingests the flow and speed data from Kafka
 *
 */
    public DataStream<Tuple3<String, String, Long>> ingestStage(StreamExecutionEnvironment env) {
        FlinkKafkaConsumer<Tuple3<String, String, Long>> kafkaSource = new FlinkKafkaConsumer<Tuple3<String, String, Long>>(
                Arrays.asList(kafkaProperty.getProperty("flow.topic"), kafkaProperty.getProperty("speed.topic")),
                new DeserializationUtil(), kafkaProperty);

        if (kafkaProperty.contains("earliest")) {
            kafkaSource.setStartFromEarliest();
        } else {
            kafkaSource.setStartFromLatest();
        }

        DataStream<Tuple3<String, String, Long>> rawStream = env.addSource(kafkaSource)
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
 * Parses the speed and flow streams
 */

}
