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

//@Service
public class PaymentRequestConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(PaymentRequestConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    @Qualifier("cachePaymentRequest")
    private Cache<String, Boolean> cachePaymentRequest;

    private boolean isExistsInCache(String key) {
        return cachePaymentRequest.getIfPresent(key) != null;
    }

    @KafkaListener(topics = "t-payment-request")
    public void consumePaymentRequest(String json) {
        try {
            var paymentRequest = objectMapper.readValue(json, PaymentRequest.class);
            var cacheKey = paymentRequest.calculateHash();

            if (isExistsInCache(cacheKey)) {
                LOG.warn("Payment request already exists in cache: {}", paymentRequest);
                return;
            }

            LOG.info("Processing payment request: {}", paymentRequest);
            cachePaymentRequest.put(cacheKey, true);
        } catch (Exception e) {
            LOG.error("Error processing payment request", e);
        }
    }

}
