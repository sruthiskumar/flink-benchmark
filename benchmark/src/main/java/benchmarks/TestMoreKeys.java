package benchmarks;

import model.FlowObservation;
import model.SpeedObservation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import util.ConfigurationUtil;

import java.util.Date;
import java.util.Properties;

public class TestMoreKeys {
    private static Properties flinkProperties = ConfigurationUtil.loadProperties("config.properties");

    public static void getKeys(DataStream<FlowObservation> flowStream) {
        flowStream.keyBy(new KeySelector<FlowObservation, Integer>() {
            @Override
            public Integer getKey(FlowObservation flowObservation) throws Exception {
                return flowObservation.timestamp.intValue();
            }
        })
                .flatMap(new RichFlatMapFunction<FlowObservation, Tuple2<Integer, Integer>>() {

                             private ValueState<Integer> flowCount;

                             @Override
                             public void flatMap(FlowObservation value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                                 Integer count = flowCount.value() != null ? flowCount.value() : 0;
                                 flowCount.update(count + 1);
                                 out.collect(new Tuple2<>(value.timestamp.intValue(), count));
                             }

                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 flowCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("ValueState", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
    }

    public static void test(DataStream<FlowObservation> flowStream, DataStream<SpeedObservation> speedStream) {
        flowStream.keyBy(new KeySelector<FlowObservation, Integer>() {
            @Override
            public Integer getKey(FlowObservation flowObservation) throws Exception {
                return flowObservation.timestamp.intValue();
            }
        })
                .flatMap(new RichFlatMapFunction<FlowObservation, Tuple2<FlowObservation, Integer>>() {

                             private ValueState<Integer> flowCount;

                             @Override
                             public void flatMap(FlowObservation value, Collector<Tuple2<FlowObservation, Integer>> out) throws Exception {
                                 Integer count = flowCount.value() != null ? flowCount.value() : 0;
                                 flowCount.update(count + 1);
                                 out.collect(new Tuple2<>(value, count));
                             }

                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 flowCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("ValueState", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
        processSpeedStream(speedStream);
    }
    public static void testRecovery(DataStream<FlowObservation> flowStream, DataStream<SpeedObservation> speedStream) {
        flowStream.keyBy(new KeySelector<FlowObservation, Integer>() {
            @Override
            public Integer getKey(FlowObservation flowObservation) throws Exception {
                return flowObservation.timestamp.intValue();
            }
        })
                .flatMap(new RichFlatMapFunction<FlowObservation, Tuple2<FlowObservation, Integer>>() {

                             private ValueState<Integer> flowCount;

                             @Override
                             public void flatMap(FlowObservation value, Collector<Tuple2<FlowObservation, Integer>> out) throws Exception {
                                 Integer count = flowCount.value() != null ? flowCount.value() : 0;
                                 if (value.timestamp.intValue() == Integer.parseInt(flinkProperties.getProperty("recovery.key")) && count > 0
                                         && (count % Integer.parseInt(flinkProperties.getProperty("recovery.value"))) == 0) {
                                     flowCount.update(count + 1);
                                     throw new FlinkRuntimeException("Exception to Recover for the key " + value.flow);
                                 }
                                 flowCount.update(count + 1);
                                 out.collect(new Tuple2<>(value, count));
                             }

                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 flowCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("ValueState", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
        processSpeedStream(speedStream);
    }

    private static void processSpeedStream(DataStream<SpeedObservation> speedStream) {
        speedStream.keyBy(new KeySelector<SpeedObservation, Double>() {
            @Override
            public Double getKey(SpeedObservation speedObservation) throws Exception {
                Date time = new Date(speedObservation.timestamp);
                return time.getSeconds()*speedObservation.speed;
            }
        })
                .flatMap(new RichFlatMapFunction<SpeedObservation, Tuple2<SpeedObservation, Integer>>() {

                             private ValueState<Integer> speedCount;

                             @Override
                             public void flatMap(SpeedObservation value, Collector<Tuple2<SpeedObservation, Integer>> out) throws Exception {
                                 Integer count = speedCount.value() != null ? speedCount.value() : 0;
                                 speedCount.update(count + 1);
                                 out.collect(new Tuple2<>(value, count));
                             }

                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 speedCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("ValueState", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
    }
}
