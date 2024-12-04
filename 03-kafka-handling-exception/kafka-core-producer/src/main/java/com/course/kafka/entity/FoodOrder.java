package com.course.kafka.entity;

public class FoodOrder {

    private int amount;

    private String foodName;

    public FoodOrder(int amount, String foodName) {
        this.amount = amount;
        this.foodName = foodName;
    }

    public FoodOrder() {
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getFoodName() {
        return foodName;
    }

    public void setFoodName(String foodName) {
        this.foodName = foodName;
    }

    @Override
    public String toString() {
        return "FoodOrder{" +
                "amount=" + amount +
                ", foodName='" + foodName + '\'' +
                '}';
    }

}
