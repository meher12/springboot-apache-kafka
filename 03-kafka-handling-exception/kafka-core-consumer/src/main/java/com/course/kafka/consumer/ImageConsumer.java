package com.course.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import com.course.kafka.entity.Image;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

//@Service
public class ImageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ImageConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "t-image", concurrency = "2", containerFactory = "imageRetryContainerFactory")
    public void consume(String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) throws Exception {
        Image image = objectMapper.readValue(message, Image.class);
        if ("svg".equalsIgnoreCase(image.getType())) {
            throw new IllegalArgumentException("SVG images are not allowed");
        }
        LOG.info("Consumed message: {} from partition: {}", message, partition);
    }
}
