package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class POSTransactionDeserializer implements Deserializer<POSTransaction> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public POSTransaction deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        POSTransaction posTransaction = null;
        try {
            posTransaction = objectMapper.readValue(data, POSTransaction.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return posTransaction;
    }

    @Override
    public void close() {

    }
}
