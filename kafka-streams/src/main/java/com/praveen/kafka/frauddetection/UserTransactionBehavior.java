package com.praveen.kafka.frauddetection;

public class UserTransactionBehavior {
    private int userId;
    private String cardNumber;
    private float avgTransactionAmount;
    private float maxTransactionAmount;
    private float avgNoOfTransactions;

    public int getUserId() {
        return userId;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    public float getAvgTransactionAmount() {
        return avgTransactionAmount;
    }

    public float getMaxTransactionAmount() {
        return maxTransactionAmount;
    }

    public float getAvgNoOfTransactions() {
        return avgNoOfTransactions;
    }

    public UserTransactionBehavior(int userId, String cardNumber, float avgTransactionAmount,
                                   float maxTransactionAmount, float avgNoOfTransactions) {
        this.userId = userId;
        this.cardNumber = cardNumber;
        this.avgTransactionAmount = avgTransactionAmount;
        this.maxTransactionAmount = maxTransactionAmount;
        this.avgNoOfTransactions = avgNoOfTransactions;
    }
}
