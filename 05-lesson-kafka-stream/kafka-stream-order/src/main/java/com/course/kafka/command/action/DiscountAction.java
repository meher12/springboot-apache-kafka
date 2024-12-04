package com.course.kafka.command.action;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.course.kafka.api.request.DiscountRequest;
import com.course.kafka.broker.message.DiscountMessage;
import com.course.kafka.broker.producer.DiscountProducer;

@Component
public class DiscountAction {

	@Autowired
	private DiscountProducer producer;

	public DiscountMessage convertToMessage(DiscountRequest request) {
		return new DiscountMessage(request.getDiscountCode(), request.getDiscountPercentage());
	}

	public void sendToKafka(DiscountMessage message) {
		producer.publish(message);
	}

}
