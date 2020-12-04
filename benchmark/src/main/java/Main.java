import benchmarks.TestFewKeys;
import benchmarks.TestLageState;
import benchmarks.TestMoreKeys;
import model.FlowObservation;
import model.SpeedObservation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import util.ConfigurationUtil;
import util.SerializationUtil;

import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic.AT_LEAST_ONCE;

public class Main {
    private static Properties flinkProperties;
    private static Properties kafkaProperty;

    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = initFlinkEnv();
        Properties kafkaProperties = initKafkaProperties();
        InitializeSetUp initializeSetUp = new InitializeSetUp(flinkProperties, kafkaProperties);
        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("metrics",
                new SerializationUtil(), kafkaProperties, AT_LEAST_ONCE);

        DataStream<Tuple3<String, String, Long>> rawStream = initializeSetUp.ingestStage(env);
        DataStream<FlowObservation> flowStream = initializeSetUp.parseFlowStreams(rawStream);
        DataStream<SpeedObservation> speedStream = initializeSetUp.parseSpeedStreams(rawStream);
//        DataStream<AggregatableObservation> aggregatableStream = initializeSetUp.joinStreams(flowStream, speedStream);
        switch (flinkProperties.getProperty("test.class")) {
            case "1":
                switch (flinkProperties.getProperty("test.number")) {
                    case "1":
                        TestFewKeys.getKeys(flowStream);
                        break;
                    case "2":
                        TestFewKeys.test(flowStream, speedStream);
                        break;
                    case "3":
                        TestFewKeys.testRecovery(flowStream, speedStream);
                }
                break;
            case "2":
                switch (flinkProperties.getProperty("test.number")) {
                    case "1":
                        TestMoreKeys.getKeys(flowStream);
                        break;
                    case "2":
                        TestMoreKeys.test(flowStream, speedStream);
                        break;
                    case "3":
                        TestMoreKeys.testRecovery(flowStream, speedStream);
                }
                break;
            case "3":
                switch (flinkProperties.getProperty("test.number")) {
                    case "1":
                        TestLageState.getKeys(flowStream);
                        break;
                    case "2":
                        TestLageState.test(flowStream, speedStream);
                        break;
                    case "3":
                        TestLageState.testRecovery(flowStream, speedStream);
                }
                break;

        }
        env.execute("Flink Traffic Analyzer");
    }

    private static StreamExecutionEnvironment initFlinkEnv() {
        flinkProperties = ConfigurationUtil.loadProperties("config.properties");
        Configuration config = new Configuration();
        config.setString("state.backend", flinkProperties.getProperty("state.backend"));
        config.setString("state.backend.ndb.connectionstring", flinkProperties.getProperty("state.backend.ndb.connectionstring"));
        config.setString("state.backend.ndb.dbname", flinkProperties.getProperty("state.backend.ndb.dbname"));
        config.setString("state.backend.ndb.truncatetableonstart", flinkProperties.getProperty("state.backend.ndb.truncatetableonstart"));


        config.setString("state.savepoints.dir", flinkProperties.getProperty("state.savepoints.dir"));
        config.setString("state.checkpoints.dir", flinkProperties.getProperty("state.checkpoints.dir"));
        config.setString("fs.s3a.access.key", flinkProperties.getProperty("s3a.access-key"));
        config.setString("fs.s3a.secret.key", flinkProperties.getProperty("s3a.secret-key"));
        config.setString("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        config.setString("fs.s3a.endpoint", flinkProperties.getProperty("fs.s3a.endpoint"));
        config.setString("web.timeout", "60000");

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
