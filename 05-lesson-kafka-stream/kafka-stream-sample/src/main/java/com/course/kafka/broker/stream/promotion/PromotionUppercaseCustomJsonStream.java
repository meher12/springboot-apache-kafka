package com.course.kafka.broker.stream.promotion;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;

import com.course.kafka.broker.message.PromotionMessage;
import com.course.kafka.broker.serde.PromotionSerde;

// @Component
public class PromotionUppercaseCustomJsonStream {

    @Autowired
    void kstreamPromotionUppercase(StreamsBuilder builder) {
        var customSerde = new PromotionSerde();
        var sourceStream = builder.stream("t-commodity-promotion", Consumed.with(Serdes.String(), customSerde));
        var uppercaseStream = sourceStream.mapValues(this::uppercasePromotionCode);

        uppercaseStream.to("t-commodity-promotion-uppercase", Produced.with(Serdes.String(), customSerde));

        sourceStream.print(Printed.<String, PromotionMessage>toSysOut().withLabel("Custom JSON Serde Original Stream"));
        uppercaseStream
                .print(Printed.<String, PromotionMessage>toSysOut().withLabel("Custom JSON Serde Uppercase Stream"));
    }

    private PromotionMessage uppercasePromotionCode(PromotionMessage message) {
        return new PromotionMessage(message.getPromotionCode().toUpperCase());
    }

}
