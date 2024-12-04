package com.course.kafka.broker.stream.flashsale;

import java.time.OffsetDateTime;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.course.kafka.broker.message.FlashSaleVoteMessage;

// @Component
public class FlashSaleVoteThreeStream {

    @Autowired
    void flashSaleVoteStream(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var flashSaleVoteSerde = new JsonSerde<>(FlashSaleVoteMessage.class);
        var voteStart = OffsetDateTime.now().plusMinutes(2);
        var voteEnd = voteStart.plusHours(1);

        builder.stream("t-commodity-flashsale-vote", Consumed.with(stringSerde, flashSaleVoteSerde))
                .processValues(
                        () -> new FlashSaleVoteThreeFixedKeyProcessor(voteStart, voteEnd))
                .filter(
                        (key, transformedValue) -> transformedValue != null)
                .map(
                        (key, value) -> KeyValue.pair(value.getCustomerId(), value.getItemName()))
                .to("t-commodity-flashsale-vote-three-user-item");

        builder.table("t-commodity-flashsale-vote-three-user-item", Consumed.with(stringSerde, stringSerde))
                .groupBy(
                        (user, votedItem) -> KeyValue.pair(votedItem, votedItem))
                .count()
                .toStream()
                .to("t-commodity-flashsale-vote-three-result");
    }

}
