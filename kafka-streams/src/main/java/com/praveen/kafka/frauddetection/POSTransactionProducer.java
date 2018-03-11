package com.praveen.kafka.frauddetection;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class POSTransactionProducer {

    private static final Logger logger = LoggerFactory.getLogger(POSTransactionProducer.class);

    public static void produce() {
        Producer<String, POSTransaction> producer = createProducer();
        List<POSTransaction> transactions = getPOSTransactions();
        for (POSTransaction transaction : transactions) {
            ProducerRecord<String, POSTransaction> record =
                    new ProducerRecord<>(
                            POSProcessingConstants.POS_TRANSACTIONS_TOPIC,
                            transaction.getCardNumber(),
                            transaction
                    );
            RecordMetadata recordMetadata = null;
            try {
                recordMetadata = producer.send(record).get();
                logger.info("Ticker sent: key={}, value={}, offset={}, partition={}", record.key(),
                        record.value(), recordMetadata.offset(), recordMetadata.partition());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    public static void produceUserDetails() {
        Producer<String, UserRecord> producer = createUserRecordProducer();
        List<UserRecord> userDetails = getUserDetails();
        for (UserRecord userRecord : userDetails) {
            ProducerRecord<String, UserRecord> record =
                    new ProducerRecord<>(
                            POSProcessingConstants.USER_RECORD_TOPIC,
                            userRecord.getCardNumber(),
                            userRecord
                    );
            RecordMetadata recordMetadata = null;
            try {
                recordMetadata = producer.send(record).get();
                logger.info("user record sent: key={}, value={}, offset={}, partition={}", record.key(),
                        record.value(), recordMetadata.offset(), recordMetadata.partition());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    private static List<UserRecord> getUserDetails() {
        return Arrays.asList(
                new UserRecord(1,"Charlie", "smith", "charlie@asdf.com", "India", "5432154321"),
                new UserRecord(2, "Alice", "smith", "alice@asdf.com", "India", "1234512345"),
                new UserRecord(3, "Bob", "smith", "bob@asdf.com", "India", "9876598765")
                );
    }

    private static List<POSTransaction> getPOSTransactions() {
        Date date = new Date();
        return Arrays.asList(

                new POSTransaction(1, "101", "1234512345",
                        new Timestamp(date.getTime()), 15000.6F,
                        "retail", "zone1"),

                new POSTransaction(2, "102", "1234512345",
                        new Timestamp(date.getTime() + 20 * 1000), 75200.6F,
                        "retail", "zone2"),

                new POSTransaction(3, "103", "1234512345",
                        new Timestamp(date.getTime() + 30 * 1000), 1345000.6F,
                        "retail", "zone3"),

                new POSTransaction(4, "104", "1234512345",
                        new Timestamp(date.getTime() + 40 * 1000), 14565000.6F,
                        "retail", "zone4"),

                new POSTransaction(5, "105", "1234512345",
                        new Timestamp(date.getTime() + 50 * 1000), 1235000.6F,
                        "retail", "zone5"),

                /*==============================================================================================*/

                new POSTransaction(1, "201", "5432154321",
                        new Timestamp(date.getTime() + 20 * 1000), 15000.6F,
                        "e-commerce", "zone1"),

                new POSTransaction(2, "202", "5432154321",
                        new Timestamp(date.getTime() + 3 * 20000), 5000.6F,
                        "e-commerce", "zone1"),

                new POSTransaction(3, "203", "5432154321",
                        new Timestamp(date.getTime() + 4 * 20000), 14000.6F,
                        "e-commerce", "zone1"),

                new POSTransaction(4, "204", "5432154321",
                        new Timestamp(date.getTime() + 6 * 20000), 20000.6F,
                        "e-commerce", "zone1"),

                new POSTransaction(5, "205", "5432154321",
                        new Timestamp(date.getTime() + 8 * 20000), 34000.6F,
                        "e-commerce", "zone1"),

                /*==============================================================================================*/

                new POSTransaction(1, "301", "9876598765",
                        new Timestamp(date.getTime() + 3 * 20000), 50000000.6F,
                        "e-commerce", "zone1"),

                new POSTransaction(1, "302", "9876598765",
                        new Timestamp(date.getTime() + 6 * 20000), 65500000.6F,
                        "e-commerce", "zone1"),

                new POSTransaction(1, "303", "9876598765",
                        new Timestamp(date.getTime() + 9 * 20000), 6785000.6F,
                        "e-commerce", "zone1"),

                new POSTransaction(1, "304", "9876598765",
                        new Timestamp(date.getTime() + 12 * 20000), 2475000.6F,
                        "e-commerce", "zone1"),

                new POSTransaction(1, "305", "9876598765",
                        new Timestamp(date.getTime() + 15 * 20000), 9865000.6F,
                        "e-commerce", "zone1")
        );
    }

    public static KafkaProducer<String, POSTransaction> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, POSProcessingConstants.FRAUD_DETECTION_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, POSProcessingConstants.FRAUD_DETECTION_CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, POSTransactionSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static KafkaProducer<String, UserRecord> createUserRecordProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, POSProcessingConstants.FRAUD_DETECTION_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, POSProcessingConstants.FRAUD_DETECTION_CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserRecordSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static KafkaProducer<String, UserRecord> createUserTransactionBehaviorDataProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, POSProcessingConstants.FRAUD_DETECTION_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, POSProcessingConstants.FRAUD_DETECTION_CLIENT_ID_CONFIG);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserRecordSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
