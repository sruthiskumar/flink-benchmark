package benchmarks;

import model.FlowObservation;
import model.SpeedObservation;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import util.ConfigurationUtil;

import java.util.Properties;

//110 keys 31 flow
public class TestFewKeys {

    private static Properties flinkProperties = ConfigurationUtil.loadProperties("config.properties");

    public static void getKeys(DataStream<FlowObservation> flowStream) {
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
                                         new ValueStateDescriptor<Integer>("TestFewKeysGetKeys", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
    }

    public static void test(DataStream<FlowObservation> flowStream, DataStream<SpeedObservation> speedStream) {
        flowStream.keyBy(x -> x.flow)
                .flatMap(new RichFlatMapFunction<FlowObservation, Tuple2<FlowObservation, Integer>>() {

                             private ValueState<Integer> flowCount;
                             //private ValueState<FlowObservation> flowObservationValueState;

                             @Override
                             public void flatMap(FlowObservation value, Collector<Tuple2<FlowObservation, Integer>> out) throws Exception {
                                 Integer count = flowCount.value() != null ? flowCount.value() : 0;
                                 flowCount.update(count + 1);
//                                 if (flowObservationValueState.value() != null) {
//                                     FlowObservation flowObservation = flowObservationValueState.value();
//                                     Integer count1 = flowObservation.count != null ? flowObservation.count : 0;
//                                     flowObservation.setCount(count1 + 1);
//                                     flowObservationValueState.update(flowObservation);
//                                 } else {
//                                     value.setCount(1);
//                                     flowObservationValueState.update(value);
//                                 }
                                 out.collect(new Tuple2<>(value, count));
                             }

                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 flowCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("TestFewKeysTest", BasicTypeInfo.INT_TYPE_INFO));
//                                 flowObservationValueState = getRuntimeContext().getState(
//                                         new ValueStateDescriptor<FlowObservation>("TestFewKeysTestFlowObservationValueState", FlowObservation.class));

                             }

                         }
                );
        processSpeedStream(speedStream);
    }

    private static void processSpeedStream(DataStream<SpeedObservation> speedStream) {
        speedStream.keyBy(x -> x.speed)
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
                                         new ValueStateDescriptor<Integer>("TestFewKeysSpeed", BasicTypeInfo.INT_TYPE_INFO));
                             }

                         }
                );
    }

    public static void testRecovery(DataStream<FlowObservation> flowStream, DataStream<SpeedObservation> speedStream) {
        flowStream.keyBy(x -> x.flow)
                .flatMap(new RichFlatMapFunction<FlowObservation, Tuple2<FlowObservation, Integer>>() {

                             private ValueState<Integer> flowCount;
                             private ValueState<FlowObservation> flowObservationValueState;


                    @Override
                             public void flatMap(FlowObservation value, Collector<Tuple2<FlowObservation, Integer>> out) throws Exception {
                                 Integer count = flowCount.value() != null ? flowCount.value() : 0;
                                 if (value.flow == Integer.parseInt(flinkProperties.getProperty("recovery.key")) && count > 0
                                         && (count % Integer.parseInt(flinkProperties.getProperty("recovery.value"))) == 0) {
                                     flowCount.update(count + 1);
                                     throw new FlinkRuntimeException("Exception to Recover for the key " + value.flow);
                                 }
                                 if (flowObservationValueState.value() != null) {
                                     FlowObservation flowObservation = flowObservationValueState.value();
                                     Integer count1 = flowObservation.count != null ? flowObservation.count : 0;
                                     flowObservation.setCount(count1 + 1);
                                     flowObservationValueState.update(flowObservation);
                                 } else {
                                     value.setCount(1);
                                     flowObservationValueState.update(value);
                                 }
                                 flowCount.update(count + 1);
                                 out.collect(new Tuple2<>(value, count));
                             }

                             @Override
                             public void open(Configuration parameters) throws Exception {

                                 flowCount = getRuntimeContext().getState(
                                         new ValueStateDescriptor<Integer>("TestFewKeysRecovery", BasicTypeInfo.INT_TYPE_INFO));
                                 flowObservationValueState = getRuntimeContext().getState(
                                         new ValueStateDescriptor<FlowObservation>("TestFewKeysRecoveryFlowObservationValueState", FlowObservation.class));
                             }

                         }
                );

        processSpeedStream(speedStream);
    }
}
