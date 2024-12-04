package com.course.kafka.broker.consumer;

import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderReplyMessage;

@Service
public class OrderWithReplyConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(OrderWithReplyConsumer.class);

    @KafkaListener(topics = "t-commodity-order")
    @SendTo("t-commodity-order-reply")
    public OrderReplyMessage consumeOrder(ConsumerRecord<String, OrderMessage> consumerRecord) {
        var headers = consumerRecord.headers();
        var orderMessage = consumerRecord.value();

        LOG.info("Kafka headers:");

        headers.forEach(header -> LOG.info("header {} : {}", header.key(), new String(header.value())));

        LOG.info("Order: {}", orderMessage);

        var bonusPercentage = Objects.isNull(headers.lastHeader("surpriseBonus")) ? 0
                : Integer.parseInt(new String(headers.lastHeader("surpriseBonus").value()));

        LOG.info("Surprise bonus is {}%", bonusPercentage);

        var orderReplyMessage = new OrderReplyMessage();
        orderReplyMessage.setReplyMessage("Order confirmed with surprise bonus " + bonusPercentage + "% from order id "
                + orderMessage.getOrderNumber());

        return orderReplyMessage;
    }

}
