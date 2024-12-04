package com.course.kafka.broker.producer;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.course.kafka.broker.message.PromotionMessage;

@Service
public class PromotionProducer {

    private static final Logger LOG = LoggerFactory.getLogger(PromotionProducer.class);

    @Autowired
    private KafkaTemplate<String, PromotionMessage> kafkaTemplate;

    public void sendPromotion(PromotionMessage message) {
        try {
            var sendResult = kafkaTemplate.send("t-commodity-promotion", message.getPromotionCode(), message)
                    .get(3, TimeUnit.SECONDS);

            LOG.info("Promotion code: {} sent successfully", sendResult.getProducerRecord().value());
        } catch (Exception e) {
            LOG.error("Error sending promotion {}", message.getPromotionCode(), e);
        }

        LOG.info("Just a dummy message for promotion {}", message.getPromotionCode());
    }

}
