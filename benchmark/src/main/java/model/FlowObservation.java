package model;

public class FlowObservation {
    public String measurementId;
    public String internalId;
    public Long timestamp;
    public Double latitude;
    public Double longitude;
    public Integer flow;
    public Integer period;
    public Integer accuracy;
    public Integer numLanes;
    public Long publishTimestamp;
    public Long ingestTimestamp;

    public FlowObservation(String measurementId, String internalId, Long timestamp, Double latitude, Double longitude, Integer flow, Integer period, Integer accuracy, Integer numLanes, Long publishTimestamp, Long ingestTimestamp) {
        this.measurementId = measurementId;
        this.internalId = internalId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.flow = flow;
        this.period = period;
        this.accuracy = accuracy;
        this.numLanes = numLanes;
        this.publishTimestamp = publishTimestamp;
        this.ingestTimestamp = ingestTimestamp;
    }

    public FlowObservation() {
        new FlowObservation("String", "String", 1L, 1.0, 1.0, 1, 1, 1, 1, 1L, 1L);
    }

    @Override
    public String toString() {
        return "FlowObservation{" +
                "measurementId='" + measurementId + '\'' +
                ", internalId='" + internalId + '\'' +
                ", timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", flow=" + flow +
                ", period=" + period +
                ", accuracy=" + accuracy +
                ", numLanes=" + numLanes +
                ", publishTimestamp=" + publishTimestamp +
                ", ingestTimestamp=" + ingestTimestamp +
                '}';
    }
}
