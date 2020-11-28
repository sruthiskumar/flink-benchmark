package model;

import java.util.Arrays;
import java.util.List;

public class AggregatableObservation {
    public String measurementId;
    public List<String> lanes;
    public Long timestamp;
    public Double latitude;
    public Double longitude;
    public Integer accumulatedFlow;
    public Integer period;
    public Integer flowAccuracy;
    public Double averageSpeed;
    public Integer speedAccuracy;
    public Integer numLanes;
    public Long publishTimestamp;
    public Long ingestTimestamp;

    public AggregatableObservation(String measurementId, List<String> lanes, Long timestamp, Double latitude, Double longitude, Integer accumulatedFlow, Integer period, Integer flowAccuracy, Double averageSpeed, Integer speedAccuracy, Integer numLanes, Long publishTimestamp, Long ingestTimestamp) {
        this.measurementId = measurementId;
        this.lanes = lanes;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.accumulatedFlow = accumulatedFlow;
        this.period = period;
        this.flowAccuracy = flowAccuracy;
        this.averageSpeed = averageSpeed;
        this.speedAccuracy = speedAccuracy;
        this.numLanes = numLanes;
        this.publishTimestamp = publishTimestamp;
        this.ingestTimestamp = ingestTimestamp;
    }

    public AggregatableObservation(FlowObservation flowObservation, SpeedObservation speedObservation) {
        this.measurementId = flowObservation.measurementId;
        this.lanes = Arrays.asList(flowObservation.internalId);
        this.timestamp = Math.max(flowObservation.timestamp, speedObservation.timestamp);
        this.latitude = flowObservation.latitude;
        this.longitude = flowObservation.longitude;
        this.accumulatedFlow = flowObservation.flow;
        this.period = flowObservation.period;
        this.flowAccuracy = flowObservation.accuracy;
        this.averageSpeed = speedObservation.speed;
        this.speedAccuracy = speedObservation.accuracy;
        this.numLanes = (flowObservation.numLanes == speedObservation.numLanes) ? flowObservation.numLanes : -1;
        this.publishTimestamp = Math.max(flowObservation.publishTimestamp, speedObservation.publishTimestamp);
        this.ingestTimestamp = Math.max(flowObservation.ingestTimestamp, speedObservation.ingestTimestamp);
    }

    public AggregatableObservation() {
    }

    @Override
    public String toString() {
        return "AggregatableObservation{" +
                "measurementId='" + measurementId + '\'' +
                ", lanes=" + lanes +
                ", timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", accumulatedFlow=" + accumulatedFlow +
                ", period=" + period +
                ", flowAccuracy=" + flowAccuracy +
                ", averageSpeed=" + averageSpeed +
                ", speedAccuracy=" + speedAccuracy +
                ", numLanes=" + numLanes +
                ", publishTimestamp=" + publishTimestamp +
                ", ingestTimestamp=" + ingestTimestamp +
                '}';
    }
}
