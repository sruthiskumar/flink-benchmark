import model.AggregatableObservation;
import model.FlowObservation;
import model.SpeedObservation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
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
//        rawStream.print();
//        rawStream.addSink(kafkaProducer);
        DataStream<FlowObservation> flowStream = initializeSetUp.parseFlowStreams(rawStream);
//        flowStream.addSink(kafkaProducer);
//        flowStream.print();
        flowObservationTestFewKeys(flowStream);
//        DataStream<SpeedObservation> speedObservationDataStream = initializeSetUp.parseSpeedStreams(rawStream);
//        speedObservationDataStream.print();
//        DataStream<AggregatableObservation> aggregatableObservationDataStream = initializeSetUp.joinStreams(flowStream, speedObservationDataStream);
//        aggregatableObservationDataStream.print();
//        flowObservationTest3(aggregatableObservationDataStream);
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

    private static void flowObservationTest1(DataStream<FlowObservation> flowStream) {
        flowStream.keyBy(new KeySelector<FlowObservation, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(FlowObservation value) {
                return Tuple2.of(value.measurementId, value.timestamp.intValue());
            }
        })
                .flatMap(new RichFlatMapFunction<FlowObservation, Tuple1<Integer>>() {

                             private ValueState<Integer> flowCount;
                             @Override
                             public void flatMap(FlowObservation value, Collector<Tuple1<Integer>> out) throws Exception {
                                 Integer count = flowCount.value() != null ? flowCount.value() : 0;
                                 flowCount.update(count + 1);
                                 out.collect(new Tuple1<>(count));
                             }
                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 flowCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("ValueState", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
    }

    // Key by flow (31 keys))
    private static void flowObservationTestFewKeys(DataStream<FlowObservation> flowStream) {
        flowStream.keyBy(x -> x.flow)
                .flatMap(new RichFlatMapFunction<FlowObservation, Tuple2<Integer, Integer>>() {

                             private ValueState<Integer> flowCount;
                             @Override
                             public void flatMap(FlowObservation value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                                 Integer count = flowCount.value() != null ? flowCount.value() : 0;
                                 flowCount.update(count + 1);
                                 out.collect(new Tuple2<>(value.flow, count));
                             }
                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 flowCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("ValueState", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
    }

    // Key by flow (31 keys))
    private static void flowObservationTestRecovery(DataStream<FlowObservation> flowStream) {
        flowStream.keyBy(x -> x.flow)
                .flatMap(new RichFlatMapFunction<FlowObservation, Tuple2<Integer, Integer>>() {

                             private ValueState<Integer> flowCount;
                             @Override
                             public void flatMap(FlowObservation value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                                 Integer count = flowCount.value() != null ? flowCount.value() : 0;
                                 if (value.flow == 1140 && count > 0 && (count % 6000 == 0)) {
                                     throw new FlinkRuntimeException("Exception to Recover for the key " + value.flow);
                                 }
                                 flowCount.update(count + 1);
                                 out.collect(new Tuple2<>(value.flow, count));
                             }
                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 flowCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("ValueState", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
    }

    private static void flowObservationTest3(DataStream<AggregatableObservation> observationDataStream) {
        observationDataStream.keyBy(new KeySelector<AggregatableObservation, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(AggregatableObservation value) throws Exception {
                return Tuple2.of(value.measurementId, value.timestamp);
            }
        }).flatMap(new RichFlatMapFunction<AggregatableObservation, Tuple1<Integer>>() {

                             private ValueState<Integer> flowCount;
                             @Override
                             public void flatMap(AggregatableObservation value, Collector<Tuple1<Integer>> out) throws Exception {
                                 Integer count = flowCount.value() != null ? flowCount.value() : 0;
                                 flowCount.update(count + 1);
                                 out.collect(new Tuple1<>(count));
                             }
                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 flowCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("ValueState", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
    }
}
