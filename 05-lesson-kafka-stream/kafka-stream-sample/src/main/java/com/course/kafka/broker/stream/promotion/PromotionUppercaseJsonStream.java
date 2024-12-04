package com.course.kafka.broker.stream.promotion;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.course.kafka.broker.message.PromotionMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

// @Component
public class PromotionUppercaseJsonStream {

    @Autowired
    private ObjectMapper objectMapper;

    private static final Logger LOG = LoggerFactory.getLogger(PromotionUppercaseJsonStream.class);

    @Autowired
    void kstreamPromotionUppercase(StreamsBuilder builder) {
        var sourceStream = builder.stream("t-commodity-promotion", Consumed.with(Serdes.String(), Serdes.String()));
        var uppercaseStream = sourceStream.mapValues(this::uppercasePromotionCode);

        uppercaseStream.to("t-commodity-promotion-uppercase");

        sourceStream.print(Printed.<String, String>toSysOut().withLabel("JSON Original Stream"));
        uppercaseStream.print(Printed.<String, String>toSysOut().withLabel("JSON Uppercase Stream"));
    }

    private String uppercasePromotionCode(String jsonString) {
        try {
            var promotion = objectMapper.readValue(jsonString, PromotionMessage.class);
            promotion.setPromotionCode(promotion.getPromotionCode().toUpperCase());
            return objectMapper.writeValueAsString(promotion);
        } catch (Exception e) {
            LOG.warn("Unable to process JSON", e);
            return "";
        }
    }

}
