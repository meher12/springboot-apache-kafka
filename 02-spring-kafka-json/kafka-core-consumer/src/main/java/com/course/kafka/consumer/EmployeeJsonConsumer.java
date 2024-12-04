package com.course.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import com.course.kafka.entity.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

//@Service
public class EmployeeJsonConsumer {

    @Autowired
    private ObjectMapper objectMapper;

    private static final Logger LOG = LoggerFactory.getLogger(EmployeeJsonConsumer.class);

    @KafkaListener(topics = "t-employee-2")
    public void consume(String message) {
        try {
            var employee = objectMapper.readValue(message, Employee.class);
            LOG.info("Employee is {}", employee);
        } catch (Exception e) {
            LOG.error("Error parsing employee", e);
        }
    }

}
