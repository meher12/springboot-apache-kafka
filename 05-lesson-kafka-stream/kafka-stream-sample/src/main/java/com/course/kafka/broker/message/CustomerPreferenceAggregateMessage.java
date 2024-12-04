package com.course.kafka.broker.message;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class CustomerPreferenceAggregateMessage {

    private Map<String, String> wishlistItems;

    private Map<String, String> shoppingCartItems;

    public CustomerPreferenceAggregateMessage(Map<String, String> wishlistItems,
            Map<String, String> shoppingCartItems) {
        this.wishlistItems = wishlistItems;
        this.shoppingCartItems = shoppingCartItems;
    }

    public CustomerPreferenceAggregateMessage() {
        this.wishlistItems = new HashMap<>();
        this.shoppingCartItems = new HashMap<>();
    }

    public void putShoppingCartItem(String itemName, OffsetDateTime lastDateTime) {
        shoppingCartItems.put(itemName, DateTimeFormatter.ISO_DATE_TIME.format(lastDateTime));
    }

    public void putWishlistItem(String itemName, OffsetDateTime lastDateTime) {
        wishlistItems.put(itemName, DateTimeFormatter.ISO_DATE_TIME.format(lastDateTime));
    }

    public Map<String, String> getWishlistItems() {
        return wishlistItems;
    }

    public void setWishlistItems(Map<String, String> wishlistItems) {
        this.wishlistItems = wishlistItems;
    }

    public Map<String, String> getShoppingCartItems() {
        return shoppingCartItems;
    }

    public void setShoppingCartItems(Map<String, String> shoppingCartItems) {
        this.shoppingCartItems = shoppingCartItems;
    }

    @Override
    public String toString() {
        return "CustomerPreferenceAggregateMessage{" +
                "wishlistItems=" + wishlistItems +
                ", shoppingCartItems=" + shoppingCartItems +
                '}';
    }

}
