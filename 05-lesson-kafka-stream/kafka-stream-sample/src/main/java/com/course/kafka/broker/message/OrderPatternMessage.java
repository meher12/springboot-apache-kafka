package com.course.kafka.broker.message;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

public class OrderPatternMessage {

    private String itemName;

    private long totalItemAmount;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ssZ")
    private OffsetDateTime orderDateTime;

    private String orderLocation;

    private String orderNumber;

    public OrderPatternMessage(String itemName, long totalItemAmount, OffsetDateTime orderDateTime,
            String orderLocation, String orderNumber) {
        this.itemName = itemName;
        this.totalItemAmount = totalItemAmount;
        this.orderDateTime = orderDateTime;
        this.orderLocation = orderLocation;
        this.orderNumber = orderNumber;
    }

    public OrderPatternMessage() {
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public long getTotalItemAmount() {
        return totalItemAmount;
    }

    public void setTotalItemAmount(long totalItemAmount) {
        this.totalItemAmount = totalItemAmount;
    }

    public OffsetDateTime getOrderDateTime() {
        return orderDateTime;
    }

    public void setOrderDateTime(OffsetDateTime orderDateTime) {
        this.orderDateTime = orderDateTime;
    }

    public String getOrderLocation() {
        return orderLocation;
    }

    public void setOrderLocation(String orderLocation) {
        this.orderLocation = orderLocation;
    }

    public String getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    @Override
    public String toString() {
        return "OrderPatternMessage{" +
                "itemName='" + itemName + '\'' +
                ", totalItemAmount=" + totalItemAmount +
                ", orderDateTime=" + orderDateTime +
                ", orderLocation='" + orderLocation + '\'' +
                ", orderNumber='" + orderNumber + '\'' +
                '}';
    }

}
