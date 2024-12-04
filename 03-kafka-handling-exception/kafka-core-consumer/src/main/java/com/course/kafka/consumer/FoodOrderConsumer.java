package com.course.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import com.course.kafka.entity.FoodOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

//@Service
public class FoodOrderConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(FoodOrderConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    private static final int MAX_AMOUNT_ORDER = 7;

    @KafkaListener(topics = "t-food-order", errorHandler = "myFoodOrderErrorHandler")
    public void consume(String message) throws Exception {
        FoodOrder foodOrder = objectMapper.readValue(message, FoodOrder.class);

        if (foodOrder.getAmount() > MAX_AMOUNT_ORDER) {
            LOG.error("Amount {} exceeds the maximum allowed amount of {}", foodOrder.getAmount(), MAX_AMOUNT_ORDER);
            throw new IllegalArgumentException("Order amount exceeds the maximum limit");
        }

        LOG.info("Consumed food order: {}", foodOrder);
    }
}