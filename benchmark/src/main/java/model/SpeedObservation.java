package model;

public class SpeedObservation {
    private String measurementId;
    private String internalId;
    private Long timestamp;
    private Double latitude;
    private Double longitude;
    private Double speed;
    private Integer accuracy;
    private Integer numLanes;
    private Long publishTimestamp;
    private Long ingestTimestamp;

    public SpeedObservation(String measurementId, String internalId, Long timestamp, Double latitude, Double longitude, Double speed, Integer accuracy, Integer numLanes, Long publishTimestamp, Long ingestTimestamp) {
        this.measurementId = measurementId;
        this.internalId = internalId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.speed = speed;
        this.accuracy = accuracy;
        this.numLanes = numLanes;
        this.publishTimestamp = publishTimestamp;
        this.ingestTimestamp = ingestTimestamp;
    }

    public SpeedObservation() {
        new SpeedObservation("String", "String", 1L, 1.0, 1.0, 1.0, 1, 1, 1L, 1L);
    }

    @Override
    public String toString() {
        return "SpeedObservation{" +
                "measurementId='" + measurementId + '\'' +
                ", internalId='" + internalId + '\'' +
                ", timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", speed=" + speed +
                ", accuracy=" + accuracy +
                ", numLanes=" + numLanes +
                ", publishTimestamp=" + publishTimestamp +
                ", ingestTimestamp=" + ingestTimestamp +
                '}';
    }
}
