package com.course.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.course.kafka.entity.CarLocation;
import com.fasterxml.jackson.databind.ObjectMapper;

//@Service
public class CarLocationProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void sendCarLocation(CarLocation carLocation) {
        try {
            String carLocationJson = objectMapper.writeValueAsString(carLocation);
            kafkaTemplate.send("t-location", carLocation.getCarId(), carLocationJson);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
