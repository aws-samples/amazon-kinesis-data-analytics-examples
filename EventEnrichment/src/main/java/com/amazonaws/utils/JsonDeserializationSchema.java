package com.amazonaws.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class JsonDeserializationSchema<T> extends AbstractDeserializationSchema<T> {
    ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> tClass;

    public JsonDeserializationSchema(Class<T> tClass) {
        super(tClass);
        this.tClass = tClass;
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, tClass);
    }
}
