package util;

import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class SerializationUtil implements KeyedSerializationSchema {

    public byte[] serializeKey(Object o) {
        return new byte[0];
    }

    public byte[] serializeValue(Object o) {
        return new byte[0];
    }

    public String getTargetTopic(Object o) {
        return null;
    }
}
