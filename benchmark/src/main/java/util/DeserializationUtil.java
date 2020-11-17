package util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

public class DeserializationUtil implements KafkaDeserializationSchema<Tuple3<String, String, Long>> {

    @Override
    public boolean isEndOfStream(Tuple3<String, String, Long> stringStringLongTuple3) {
        return false;
    }

    @Override
    public Tuple3<String, String, Long> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        String key = new String(consumerRecord.key(), StandardCharsets.UTF_8);
        String value = new String(consumerRecord.value(), StandardCharsets.UTF_8);
        return new Tuple3<>(key, value, consumerRecord.timestamp());
    }

    @Override
    public TypeInformation<Tuple3<String, String, Long>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>(){});
    }
}
