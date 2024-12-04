package com.course.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

//@Service
public class FixeRateConsumer {

    public static final Logger LOG = LoggerFactory.getLogger(FixeRateConsumer.class);

    @KafkaListener(topics = "t-fixedrate")
    public void consumer(String message){
        LOG.info("Consuming message: {}", message);
    }
}
