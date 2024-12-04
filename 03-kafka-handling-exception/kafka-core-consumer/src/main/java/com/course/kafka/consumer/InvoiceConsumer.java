package com.course.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import com.course.kafka.entity.Invoice;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

//@Service
public class InvoiceConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(InvoiceConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "t-invoice", concurrency = "2", containerFactory = "invoiceDltContainerFactory")
    public void consume(String message) throws Exception {
        var invoice = objectMapper.readValue(message, Invoice.class);

        if (invoice.getAmount() < 1) {
            throw new IllegalArgumentException("Invalid amount :" + invoice.getAmount());
        }

        LOG.info("Consumed invoice : {}", invoice);
    }

}
