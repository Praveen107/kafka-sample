package com.praveen.kafka.frauddetection;

public class POSProcessingConstants {
    public static final String POS_TRANSACTIONS_TOPIC = "pos-transaction-feed";
    public static final String USER_RECORD_TOPIC = "user-records";
    public static final String USER_TRANSACTION_BEHAVIOR_TOPIC = "user-transaction-behavior";
    public static final String POS_TRANSACTIONS_PROCESSING_TOPIC = "pos-transaction-processing";
    public static final String FRAUD_TRANSACTIONS_TOPIC = "pos-fraud-transaction-processing";
    public static final String FRAUD_DETECTION_BOOTSTRAP_SERVERS = "kafka-cluster:9092";
    public static final String FRAUD_DETECTION_CLIENT_ID_CONFIG = "pos-transactions-fraud-detection";
    public static final String FRAUD_DETECTION_AUTO_COMMIT_INTERVAL_MS_CONFIG = "1000";
    public static final String FRAUD_DETECTIONGROUP_ID_CONFIG = "pos-transactions-fraud-detection";
    public static final String FRAUD_DETECTION_APPLICATION_ID_CONFIG = "pos-transactions-fraud-detection-application";
    public static final String FRAUD_DETECTION_STATE_STORE_DIR = "pos-transactions-fraud-detection-state-store";


}
