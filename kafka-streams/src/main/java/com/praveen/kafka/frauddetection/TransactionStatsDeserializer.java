package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class TransactionStatsDeserializer implements Deserializer<POSTransactionStats> {
    public TransactionStatsDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public POSTransactionStats deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        POSTransactionStats posTransactionStats = null;
        try {
            posTransactionStats = objectMapper.readValue(data, POSTransactionStats.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return posTransactionStats;
    }

    @Override
    public void close() {

    }
}
