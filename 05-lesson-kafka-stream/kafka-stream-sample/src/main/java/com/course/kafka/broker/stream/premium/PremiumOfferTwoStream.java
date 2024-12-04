package com.course.kafka.broker.stream.premium;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.course.kafka.broker.message.PremiumOfferMessage;
import com.course.kafka.broker.message.PremiumPurchaseMessage;
import com.course.kafka.broker.message.PremiumUserMessage;

// @Component
public class PremiumOfferTwoStream {

    private PremiumOfferMessage joiner(PremiumPurchaseMessage purchase, PremiumUserMessage user) {
        var result = new PremiumOfferMessage();

        result.setUsername(purchase.getUsername());
        result.setPurchaseNumber(purchase.getPurchaseNumber());

        if (user != null) {
            result.setLevel(user.getLevel());
        }

        return result;
    }

    @Autowired
    void kstreamPremiumOffer(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var purchaseSerde = new JsonSerde<>(PremiumPurchaseMessage.class);
        var userSerde = new JsonSerde<>(PremiumUserMessage.class);
        var offerSerde = new JsonSerde<>(PremiumOfferMessage.class);

        var purchaseStream = builder.stream("t-commodity-premium-purchase",
                Consumed.with(stringSerde, purchaseSerde)).selectKey(
                        (k, v) -> v.getUsername());

        var filterLevel = java.util.List.of("gold", "diamond");

        var userTable = builder.table("t-commodity-premium-user",
                Consumed.with(stringSerde, userSerde))
                .filter((k, v) -> filterLevel.contains(v.getLevel().toLowerCase()));

        purchaseStream.leftJoin(userTable, this::joiner, Joined.with(stringSerde, purchaseSerde, userSerde))
                .peek(
                        (k, v) -> System.out.println("Key = " + k + " Value = " + v))
                .filter(
                        (k, v) -> v.getLevel() != null)
                .to("t-commodity-premium-offer-two", Produced.with(stringSerde, offerSerde));
    }

}
