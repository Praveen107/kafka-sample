package com.praveen.first.kafkaStreams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.praveen.kafka.frauddetection.POSTransactionStats;
import com.praveen.kafka.frauddetection.TransactionStatsDeserializer;
import com.praveen.kafka.frauddetection.TransactionStatsSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class TransactionStatsSerdes implements Serde<POSTransactionStats> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<POSTransactionStats> serializer() {
        return new TransactionStatsSerializer();
    }

    @Override
    public Deserializer<POSTransactionStats> deserializer() {
        return new TransactionStatsDeserializer();
    }
}
