package com.course.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import com.course.kafka.entity.SimpleNumber;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

//@Service
public class SimpleNumberProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void sendSimpleNumber(SimpleNumber simpleNumber) {
        try {
            String json = objectMapper.writeValueAsString(simpleNumber);
            kafkaTemplate.send("t-simple-number", json);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
