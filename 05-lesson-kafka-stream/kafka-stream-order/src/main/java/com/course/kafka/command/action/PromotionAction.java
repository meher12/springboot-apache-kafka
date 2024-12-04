package com.course.kafka.command.action;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.course.kafka.broker.producer.PromotionProducer;
import com.course.kafka.api.request.PromotionRequest;
import com.course.kafka.broker.message.PromotionMessage;

@Component
public class PromotionAction {

    @Autowired
    private PromotionProducer producer;

    public PromotionMessage convertToPromotionMessage(PromotionRequest request) {
        return new PromotionMessage(request.getPromotionCode());
    }

    public void sendToKafka(PromotionMessage message) {
        producer.sendPromotion(message);
    }
}