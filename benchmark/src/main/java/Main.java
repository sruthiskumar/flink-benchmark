import model.FlowObservation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import util.ConfigurationUtil;
import util.SerializationUtil;

import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.EXACTLY_ONCE;

public class Main {
    private static Properties flinkProperties;
    private static Properties kafkaProperty;

    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = initFlinkEnv();
        Properties kafkaProperties = initKafkaProperties();
        InitializeSetUp initializeSetUp = new InitializeSetUp(flinkProperties, kafkaProperties);
        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("metrics",
                new SerializationUtil(), kafkaProperties);

        DataStream<Tuple3<String, String, Long>> rawStream = initializeSetUp.ingestStage(env);
//        DataStream<Tuple2<String, Long>> mapped =
//                rawStream.map(new MapFunction<Tuple3<String, String, Long>, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(Tuple3<String, String, Long> stringStringLongTuple3) throws Exception {
//                        return new Tuple2<String, Long>(stringStringLongTuple3.f0, stringStringLongTuple3.f2);
//                    }
//                });

        rawStream.addSink(kafkaProducer);
        rawStream.print();
        DataStream<FlowObservation> flowStream = initializeSetUp.parseFlowStreams(rawStream);
        flowStream.addSink(kafkaProducer);
        flowStream.print();
        env.execute("Flink Traffic Analyzer");
    }

    private static StreamExecutionEnvironment initFlinkEnv() {
        flinkProperties = ConfigurationUtil.loadProperties("config.properties");
        Configuration config = new Configuration();
        //config.setString("state.backend","filesystem");
        //config.setString("state.backend", "rocksdb");
        config.setString("state.backend", flinkProperties.getProperty("state.backend"));
        config.setString("state.backend.ndb.connectionstring", flinkProperties.getProperty("state.backend.ndb.connectionstring"));
        config.setString("state.backend.ndb.dbname", flinkProperties.getProperty("state.backend.ndb.dbname"));
        config.setString("state.backend.ndb.truncatetableonstart", flinkProperties.getProperty("state.backend.ndb.truncatetableonstart"));


        config.setString("state.savepoints.dir", flinkProperties.getProperty("state.savepoints.dir"));
        config.setString("state.checkpoints.dir", flinkProperties.getProperty("state.checkpoints.dir"));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(Integer.parseInt(flinkProperties.getProperty("parallelism")));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(Integer.parseInt(flinkProperties.getProperty("auto.watermark.interval")));
        env.setBufferTimeout(Integer.parseInt(flinkProperties.getProperty("buffer.timeout")));
        env.enableCheckpointing(Integer.parseInt(flinkProperties.getProperty("checkpoint.interval")));
        //env.getConfig().enableObjectReuse();

        return env;
    }

    private static Properties initKafkaProperties() {
        kafkaProperty = ConfigurationUtil.loadProperties("kafka.properties");
        String timeToString = "FLINK/" + System.currentTimeMillis();
        kafkaProperty.setProperty("group.id", timeToString);
        return kafkaProperty;
    }
}
