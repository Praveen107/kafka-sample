package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class UserRecordDeserializer implements Deserializer<UserRecord> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public UserRecord deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        UserRecord userRecord = null;
        try {
            userRecord = objectMapper.readValue(data, UserRecord.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return userRecord;
    }

    @Override
    public void close() {

    }
}
