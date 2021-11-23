package com.amazonaws.services.kinesisanalytics;

public class TestRecord {
    private int testInt;
    private byte[] randomBytes;

    public TestRecord() {
    }

    public TestRecord(int testInt) {
        this.testInt = testInt;
    }

    public byte[] getRandomBytes() {
        return randomBytes;
    }

    public void setRandomBytes(byte[] randomBytes) {
        this.randomBytes = randomBytes;
    }

    public int getTestInt() {
        return testInt;
    }

    public void setTestInt(int testInt) {
        this.testInt = testInt;
    }
}
