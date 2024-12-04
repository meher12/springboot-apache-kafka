package com.course.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;

import com.course.kafka.entity.PaymentRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.stereotype.Service;

@Service
public class PaymentRequest2Consumer {

    private static final Logger LOG = LoggerFactory.getLogger(PaymentRequest2Consumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    @Qualifier("cachePaymentRequest")
    private Cache<String, Boolean> cachePaymentRequest;

    @KafkaListener(topics = "t-payment-request", containerFactory = "paymentRequestContainerFactory")
    public void consumePaymentRequest(String json) {
        try {
            var paymentRequest = objectMapper.readValue(json, PaymentRequest.class);
            var cacheKey = paymentRequest.calculateHash();

            LOG.info("Processing payment request: {}", paymentRequest);
            cachePaymentRequest.put(cacheKey, true);
        } catch (Exception e) {
            LOG.error("Error processing payment request", e);
        }
    }

}
