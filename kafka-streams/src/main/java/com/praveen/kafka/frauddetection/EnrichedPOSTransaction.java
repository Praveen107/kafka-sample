package com.praveen.kafka.frauddetection;

import java.sql.Timestamp;

public class EnrichedPOSTransaction {
    private UserRecord userRecord;
    private POSTransaction posTransaction;

    public UserRecord getUserRecord() {
        return userRecord;
    }

    public POSTransaction getPosTransaction() {
        return posTransaction;
    }

    public EnrichedPOSTransaction(UserRecord userRecord, POSTransaction posTransaction) {
        this.userRecord = userRecord;
        this.posTransaction = posTransaction;
    }

    public EnrichedPOSTransaction() {
    }

    @Override
    public String toString() {
        return "EnrichedPOSTransaction{" +
                "userRecord=" + userRecord.toString() +
                ", posTransaction=" + posTransaction.toString() +
                '}';
    }
}

