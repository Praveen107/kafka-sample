package com.praveen.kafka.frauddetection;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.praveen.first.kafkaStreams.TransactionStatsSerdes;
import javafx.util.Pair;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.serialization.Serdes.serdeFrom;

public class POSTransactionProcessor {
    public static void main(String[] args) {
        start();
    }

    public static void start() {
        KafkaStreams streams = createStream();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static KafkaStreams createStream() {
        Serde<POSTransaction> POSTransactionSerde = serdeFrom(new POSTransactionSerializer(), new POSTransactionDeserializer());
        Serde<UserRecord> userRecordSerde = serdeFrom(new UserRecordSerializer(), new UserRecordDeserializer());
        Serde<UserTransactionBehavior> userTransactionBehaviorSerde = serdeFrom(new UserTransactionBehaviorSerializer(), new UserTransactionBehaviorDeserializer());
        Serde<POSTransactionStats> POSTransactionStatsSerde = serdeFrom(new TransactionStatsSerializer(), new TransactionStatsDeserializer());
        TransactionStatsSerdes transactionStatsSerdes = new TransactionStatsSerdes();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, POSProcessingConstants.FRAUD_DETECTION_APPLICATION_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, POSProcessingConstants.FRAUD_DETECTION_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, POSProcessingConstants.FRAUD_DETECTION_STATE_STORE_DIR);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, POSTransaction> transactionKStream = builder.stream(POSProcessingConstants.POS_TRANSACTIONS_TOPIC,
                Consumed.with(Serdes.String(), POSTransactionSerde));

        KTable<String, UserRecord> userRecordsTable = builder.table(POSProcessingConstants.USER_RECORD_TOPIC,
                Consumed.with(Serdes.String(), userRecordSerde));

        KTable<String, UserTransactionBehavior> userTransactionBehaviorKTable = builder.table(
                POSProcessingConstants.USER_TRANSACTION_BEHAVIOR_TOPIC,
                Consumed.with(Serdes.String(), userTransactionBehaviorSerde));

        KStream<String, EnrichedPOSTransaction> enrichedPOSTransactionKStream = transactionKStream.leftJoin(userRecordsTable,
                (leftVal, rightVal) -> new EnrichedPOSTransaction(rightVal, leftVal));

        enrichedPOSTransactionKStream.print(Printed.toSysOut());


//        KTable<Windowed<String>, Long> aggregate = stringEnrichedPOSTransactionTimeWindowedKStream.aggregate(() -> 0L,
//                (aggKey, newVal, aggVal) -> aggVal.longValue() + newVal.getPosTransaction().getTransactionAmount().longValue());

//        KTable<Windowed<String>, POSTransactionStats> aggregate1 = enrichedPOSTransactionKStream.groupByKey().aggregate(POSTransactionStats::new,
//                (k, v, tradestats) -> tradestats.add(v),
//                TimeWindows.of(TimeUnit.DAYS.toMillis(1)),
//                POSTransactionStatsSerde,
//                "transaction-stats-store");
//
//        aggregate1.toStream().print(Printed.toSysOut());


//        KStream<TickerWindow, POSTransactionStats> tickerWindowPOSTransactionStatsKStream =
        TimeWindowedKStream<String, EnrichedPOSTransaction> stringEnrichedPOSTransactionTimeWindowedKStream1 =
                enrichedPOSTransactionKStream.groupByKey().
                windowedBy(TimeWindows.of(TimeUnit.DAYS.toMillis(1)));

        stringEnrichedPOSTransactionTimeWindowedKStream1.count().toStream().print(Printed.toSysOut());

//        KTable<Windowed<String>, POSTransactionStats> aggregate = stringEnrichedPOSTransactionTimeWindowedKStream1.aggregate(POSTransactionStats::new,
//                (k, v, tradestats) -> {
//                    tradestats.add(v);
//                    return tradestats;
//                },
//                Materialized.<String, POSTransactionStats, WindowStore<Bytes, byte[]>>as("sessionized-aggregated-stream-store") /* state store name */
//                        .withValueSerde(transactionStatsSerdes));
//
//        aggregate.toStream().print(Printed.toSysOut());


        //toStream((key, value) -> new TickerWindow(key.key(), key.window().start())).mapValues((stats) -> stats.computeAvgAmount());

//        KStream<TickerWindow, Float> avgrageFeed = tickerWindowPOSTransactionStatsKStream.mapValues((values) -> values.avgAmount);
//        avgrageFeed.print(Printed.toSysOut());

//        KStream<String, Tuple<EnrichedPOSTransaction, UserTransactionBehavior>> transactionWithUserHistoryDataStream =
//                EnrichedPOSTransactionTimeWindowedKStream.
////                        .leftJoin(userTransactionBehaviorKTable,
////                (leftVal, rightVal) -> new Tuple<EnrichedPOSTransaction, UserTransactionBehavior>(leftVal, rightVal));

//        KTable<Windowed<String>, POSTransactionStats> aggregate = enrichedPOSTransactionKStream.groupByKey().aggregate(POSTransactionStats::new,
//                (k, v, tradestats) -> tradestats.add(v),
//                TimeWindows.of(TimeUnit.DAYS.toMillis(1)),
//                POSTransactionStatsSerde,
//                "transaction-stats-store");


        return new KafkaStreams(builder.build(), props);

    }
}

class TickerWindow {
    String ticker;
    long timestamp;

    public TickerWindow(String ticker, long timestamp) {
        this.ticker = ticker;
        this.timestamp = timestamp;
    }
}

