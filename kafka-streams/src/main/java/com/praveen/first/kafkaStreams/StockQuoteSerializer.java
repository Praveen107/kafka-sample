package com.praveen.first.kafkaStreams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StockQuoteSerializer implements Serializer<StockQuote> {

    @Override
    public byte[] serialize(String topic, StockQuote stockQuote) {
        byte[] data = new byte[0];
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            data = objectMapper.writeValueAsString(stockQuote).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return data;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
