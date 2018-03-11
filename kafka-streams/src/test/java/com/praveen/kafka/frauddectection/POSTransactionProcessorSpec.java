package com.praveen.kafka.frauddectection;

import com.praveen.kafka.frauddetection.POSTransaction;
import com.praveen.kafka.frauddetection.POSTransactionConsumer;
import com.praveen.kafka.frauddetection.POSTransactionProcessor;
import com.praveen.kafka.frauddetection.POSTransactionProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class POSTransactionProcessorSpec {

    @Test
    public void shouldFilterStockQuoteFeed() throws Exception {
        POSTransactionProducer.produceUserDetails();
        POSTransactionProducer.produce();
        KafkaStreams stream = POSTransactionProcessor.createStream();
        stream.start();
        ArrayList<ConsumerRecord<String, POSTransaction>> fraudTrnasactions = POSTransactionConsumer.consume(10);
        Assert.assertTrue(fraudTrnasactions.size() == 15    );
//        Thread.sleep(5000);
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
//
//    @Test
//    public void shouldReturnStockLiquidityData() throws Exception {
//        StockPriceProducer.produce(100, 0);
//        KafkaStreams stream = StockPriceProcessing.createStream();
//        stream.start();
//
//        ArrayList<ConsumerRecord<String, Long>> processedStockFeeds =
//                StockPriceConsumer.getStockLiquidityData(10);
//        Assert.assertTrue(processedStockFeeds.size() <= 6);
//        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
//    }
}
