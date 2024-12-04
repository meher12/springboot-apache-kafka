package com.course.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import com.course.kafka.entity.FoodOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

//@Service
public class FoodOrderProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void sendFoodOrder(FoodOrder foodOrder) {
        try {
            var json = objectMapper.writeValueAsString(foodOrder);
            kafkaTemplate.send("t-food-order", json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
