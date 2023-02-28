package com.amazonaws.services.kinesisanalytics.domain;

import com.amazonaws.services.kinesisanalytics.avro.RoomTemperature;
import com.amazonaws.services.kinesisanalytics.avro.TemperatureSample;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RoomAverageTemperatureCalculatorTest {

    RoomAverageTemperatureCalculator sut = new RoomAverageTemperatureCalculator();

    @Test
    void createAccumulator() {
        RoomTemperature roomTemperature = sut.createAccumulator();

        assertNotNull(roomTemperature);
        assertEquals(0.0f, roomTemperature.getTemperature());
        assertEquals(0, roomTemperature.getSampleCount());
        assertEquals(Instant.EPOCH, roomTemperature.getLastSampleTime());
    }

    @Test
    void add() {
        TemperatureSample sample = TemperatureSample.newBuilder()
                .setSensorId(42)
                .setRoom("a-room")
                .setSampleTime(Instant.ofEpochMilli(50))
                .setTemperature(10.0f)
                .build();
        RoomTemperature acc = RoomTemperature.newBuilder()
                .setRoom("a-room")
                .setSampleCount(10)
                .setTemperature(20.0f)
                .setLastSampleTime(Instant.ofEpochMilli(100))
                .build();

        RoomTemperature accOut = sut.add(sample, acc);

        assertEquals("a-room", accOut.getRoom());
        assertEquals(10 + 1, accOut.getSampleCount());
        assertEquals(Instant.ofEpochMilli(100), accOut.getLastSampleTime());
        assertEquals((20.0f * 10 + 10.0f) / (float) (10 + 1), accOut.getTemperature());
    }

    @Test
    void getResult() {
        RoomTemperature acc = RoomTemperature.newBuilder()
                .setRoom("a-room")
                .setSampleCount(10)
                .setTemperature(20.0f)
                .setLastSampleTime(Instant.ofEpochMilli(100))
                .build();

        RoomTemperature result = sut.getResult(acc);
        assertEquals(acc.getRoom(), result.getRoom());
        assertEquals(acc.getTemperature(), result.getTemperature());
        assertEquals(acc.getSampleCount(), result.getSampleCount());
        assertEquals(acc.getLastSampleTime(), acc.getLastSampleTime());
    }

    @Test
    void merge() {
        RoomTemperature a = RoomTemperature.newBuilder()
                .setRoom("a-room")
                .setSampleCount(10)
                .setTemperature(20.0f)
                .setLastSampleTime(Instant.ofEpochMilli(100))
                .build();
        RoomTemperature b = RoomTemperature.newBuilder()
                .setRoom("a-room")
                .setSampleCount(20)
                .setTemperature(40.0f)
                .setLastSampleTime(Instant.ofEpochMilli(200))
                .build();

        RoomTemperature c = sut.merge(a, b);
        assertEquals("a-room", c.getRoom());
        assertEquals(10 + 20, c.getSampleCount());
        assertEquals((20.0f * 10 + 40.0f * 20) / (float) (10 + 20), c.getTemperature());
        assertEquals(Instant.ofEpochMilli(200), c.getLastSampleTime());
    }
}