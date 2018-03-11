package com.praveen.kafka.frauddetection;

public class POSTransactionStats {
    float avgAmount;
    float totalAmount;
    int noOfTransacions;
    float maxAmount;

    public POSTransactionStats add(EnrichedPOSTransaction transaction) {
        noOfTransacions += 1;
        totalAmount += transaction.getPosTransaction().getTransactionAmount();
        maxAmount = transaction.getPosTransaction().getTransactionAmount() > maxAmount ?
                transaction.getPosTransaction().getTransactionAmount() : maxAmount;
        return  this;
    }

    public POSTransactionStats computeAvgAmount() {
        this.avgAmount = this.totalAmount/this.noOfTransacions;
        return  this;
    }

    public POSTransactionStats() {
    }

    @Override
    public String toString() {
        return "POSTransactionStats{" +
                "avgAmount=" + avgAmount +
                ", totalAmount=" + totalAmount +
                ", noOfTransacions=" + noOfTransacions +
                ", maxAmount=" + maxAmount +
                '}';
    }
}
