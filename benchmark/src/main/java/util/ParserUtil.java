package util;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import model.FlowObservation;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;

public class ParserUtil {
        public static FlowObservation parseLineFlowObservation(String key, String value, Long time) throws ParseException {
            JsonObject jsonObject = new JsonParser().parse(value).getAsJsonObject();
            Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").parse(jsonObject.get("timestamp").getAsString()).getTime();
            FlowObservation flowObservation
                    = new FlowObservation(key.substring(0, key.lastIndexOf("/")),
                    key.substring(key.lastIndexOf("/") + 1),
                    timeStamp,
                    jsonObject.get("lat").getAsDouble(),
                    jsonObject.get("long").getAsDouble(),
                    jsonObject.get("flow").getAsInt(),
                    jsonObject.get("period").getAsInt(),
                    jsonObject.get("accuracy").getAsInt(),
                    jsonObject.get("num_lanes").getAsInt(),
                    time,
                    Instant.now().toEpochMilli());
            return flowObservation;
        }
}
