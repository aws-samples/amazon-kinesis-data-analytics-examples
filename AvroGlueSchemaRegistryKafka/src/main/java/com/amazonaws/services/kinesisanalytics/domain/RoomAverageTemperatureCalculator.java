package com.amazonaws.services.kinesisanalytics.domain;

import com.amazonaws.services.kinesisanalytics.avro.RoomTemperature;
import com.amazonaws.services.kinesisanalytics.avro.TemperatureSample;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.Instant;

/**
 * AggregateFunction aggregating the room average temperature
 * (provided only for completeness, but not relevant for the AVRO-Glue Schema Registry example)
 */
public class RoomAverageTemperatureCalculator implements AggregateFunction<TemperatureSample, RoomTemperature, RoomTemperature> {
    private Instant maxInstant(Instant a, Instant b) {
        return Instant.ofEpochMilli(
                Math.max(a.toEpochMilli(), b.toEpochMilli()));
    }

    @Override
    public RoomTemperature createAccumulator() {
        return RoomTemperature.newBuilder()
                .setRoom("").setSampleCount(0).setLastSampleTime(Instant.EPOCH).setTemperature(0).build();
    }

    @Override
    public RoomTemperature add(TemperatureSample sample, RoomTemperature accumulator) {
        final int sampleCount = accumulator.getSampleCount();
        final float roomAvgTemp = accumulator.getTemperature();
        final Instant lastSampleTime = maxInstant(accumulator.getLastSampleTime(), sample.getSampleTime());
        final float newRoomAvgTemp = (roomAvgTemp * sampleCount + sample.getTemperature()) / (float) (sampleCount + 1);

        accumulator.setRoom(sample.getRoom());
        accumulator.setSampleCount(sampleCount + 1);
        accumulator.setTemperature(newRoomAvgTemp);
        accumulator.setLastSampleTime(lastSampleTime);

        return accumulator;
    }

    @Override
    public RoomTemperature getResult(RoomTemperature accumulator) {
        return accumulator;
    }

    @Override
    public RoomTemperature merge(RoomTemperature a, RoomTemperature b) {
        final int totalSampleCount = a.getSampleCount() + b.getSampleCount();
        final float avgTemperature = (a.getTemperature() * a.getSampleCount() + b.getTemperature() * b.getSampleCount()) / (float) (totalSampleCount);

        return RoomTemperature.newBuilder()
                .setRoom(a.getRoom())
                .setSampleCount(totalSampleCount)
                .setTemperature(avgTemperature)
                .setLastSampleTime(maxInstant(a.getLastSampleTime(), b.getLastSampleTime()))
                .build();
    }


}
