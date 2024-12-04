package com.course.kafka.broker.stream.promotion;

import com.course.kafka.broker.message.PromotionMessage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class PromotionUppercaseJsonStreamWithAI {

    @Bean
    public KStream<String, PromotionMessage> kStreamPromotionUppercase(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var promotionMessageSerde = new JsonSerde<>(PromotionMessage.class);

        KStream<String, PromotionMessage> sourceStream = builder.stream("t-commodity-promotion",
                Consumed.with(stringSerde, promotionMessageSerde));

        KStream<String, PromotionMessage> uppercaseStream = sourceStream.mapValues(promotionMessage -> {
            System.out.println("Original: " + promotionMessage);
            promotionMessage.setPromotionCode(promotionMessage.getPromotionCode().toUpperCase());
            System.out.println("Processed: " + promotionMessage);
            return promotionMessage;
        });

        uppercaseStream.to("t-commodity-promotion-uppercase", Produced.with(stringSerde, promotionMessageSerde));

        return sourceStream;
    }
}