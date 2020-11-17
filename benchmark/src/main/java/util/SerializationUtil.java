package util;

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.UUID;

public class SerializationUtil implements KeyedSerializationSchema<String> {

    @Override
    public byte[] serializeKey(String s) {
        String key = "flink" + UUID.randomUUID().toString();
        return key.getBytes();
    }

    @Override
    public byte[] serializeValue(String s) {
        return s.getBytes();
    }

    @Override
    public String getTargetTopic(String s) {
        return "metrics";
    }
}
