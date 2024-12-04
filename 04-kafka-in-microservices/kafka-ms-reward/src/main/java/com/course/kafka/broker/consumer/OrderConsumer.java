package com.course.kafka.broker.consumer;

import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

import com.course.kafka.broker.message.OrderMessage;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = "t-commodity-order")
    public void consumeOrder(ConsumerRecord<String, OrderMessage> consumerRecord) {
        var headers = consumerRecord.headers();
        var orderMessage = consumerRecord.value();

        LOG.info("Kafka headers:");

        headers.forEach(header -> LOG.info("header {} : {}", header.key(), new String(header.value())));

        LOG.info("Order: {}", orderMessage);

        var bonusPercentage = Objects.isNull(headers.lastHeader("surpriseBonus")) ? 0
                : Integer.parseInt(new String(headers.lastHeader("surpriseBonus").value()));

        LOG.info("Surprise bonus is {}%", bonusPercentage);
    }

}
