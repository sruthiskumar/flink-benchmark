package util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class DeserializationUtil implements KafkaDeserializationSchema {
    public boolean isEndOfStream(Object o) {
        return false;
    }

    public Object deserialize(ConsumerRecord consumerRecord) throws Exception {
        return null;
    }

    public TypeInformation<Tuple3<String, String, Long>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){});
    }
}
