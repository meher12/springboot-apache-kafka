package com.course.kafka.api.request;

import java.util.List;

public class OrderRequest {

    private String orderLocation;

    private String creditCardNumber;

    private List<OrderItemRequest> items;

    public OrderRequest() {
        // No-argument constructor
    }

    public OrderRequest(String orderLocation, String creditCardNumber, List<OrderItemRequest> items) {
        this.orderLocation = orderLocation;
        this.creditCardNumber = creditCardNumber;
        this.items = items;
    }

    // Getters and Setters
    public String getOrderLocation() {
        return orderLocation;
    }

    public void setOrderLocation(String orderLocation) {
        this.orderLocation = orderLocation;
    }

    public String getCreditCardNumber() {
        return creditCardNumber;
    }

    public void setCreditCardNumber(String creditCardNumber) {
        this.creditCardNumber = creditCardNumber;
    }

    public List<OrderItemRequest> getItems() {
        return items;
    }

    public void setItems(List<OrderItemRequest> items) {
        this.items = items;
    }

    // toString() method
    @Override
    public String toString() {
        return "OrderRequest{" +
                "orderLocation='" + orderLocation + '\'' +
                ", creditCardNumber='" + creditCardNumber + '\'' +
                ", items=" + items +
                '}';
    }

}
