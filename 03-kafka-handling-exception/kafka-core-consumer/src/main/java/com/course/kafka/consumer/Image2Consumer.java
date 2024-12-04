package com.course.kafka.consumer;

import com.course.kafka.entity.Image;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

//@Service
public class Image2Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(Image2Consumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @RetryableTopic(autoCreateTopics = "true", attempts = "4",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            backoff = @Backoff(delay = 3000, maxDelay = 10000, multiplier = 1.5, random = true), dltTopicSuffix = "-dead")
    @KafkaListener(topics = "t-image-2", concurrency = "2")
    public void consume(String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) throws Exception {
        Image image = objectMapper.readValue(message, Image.class);
        if ("svg".equalsIgnoreCase(image.getType())) {
            LOG.warn("Image type is SVG : {}", image);
            throw new IllegalArgumentException("SVG images are not allowed");
        }
        LOG.info("Consumed message: {} from partition: {}", message, partition);
    }
}
