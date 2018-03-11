package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class UserTransactionBehaviorDeserializer implements Deserializer<UserTransactionBehavior> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public UserTransactionBehavior deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        UserTransactionBehavior userTransactionBehavior = null;
        try {
            userTransactionBehavior = objectMapper.readValue(data, UserTransactionBehavior.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return userTransactionBehavior;
    }

    @Override
    public void close() {

    }
}
