package com.course.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import com.course.kafka.entity.Commodity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

//@Service
public class CommodityNotificationConsumer {

    @Autowired
    private ObjectMapper objectMapper;

    private static final Logger LOG = LoggerFactory.getLogger(CommodityNotificationConsumer.class);

    @KafkaListener(topics = "t-commodity", groupId = "consumer-group-notification")
    public void listen(String message) {
        try {
            Commodity commodity = objectMapper.readValue(message, Commodity.class);
            LOG.info("Notification consumer: {}", commodity);
        } catch (Exception e) {
            LOG.error("Error processing message", e);
        }
    }

}
