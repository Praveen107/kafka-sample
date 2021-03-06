package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class UserRecordSerializer implements Serializer<UserRecord> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, UserRecord data) {
        byte[] bytes = new byte[0];
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            bytes = objectMapper.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return bytes;
    }

    @Override
    public void close() {

    }
}
