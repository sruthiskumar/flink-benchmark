package model;

public class SpeedObservation {
    public String measurementId;
    public String internalId;
    public Long timestamp;
    public Double latitude;
    public Double longitude;
    public Double speed;
    public Integer accuracy;
    public Integer numLanes;
    public Long publishTimestamp;
    public Long ingestTimestamp;
    public Long[] dummyArray;
    public Long[] getDummyArray() {
        return dummyArray;
    }

    public void setDummyArray() {
        this.dummyArray = new Long[5000];
        for(int i = 0; i < 5000; i++) {
            this.dummyArray[i] = Long.valueOf(i);
        }
    }


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
