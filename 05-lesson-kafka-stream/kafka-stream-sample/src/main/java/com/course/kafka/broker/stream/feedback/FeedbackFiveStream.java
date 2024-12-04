package com.course.kafka.broker.stream.feedback;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.course.kafka.broker.message.FeedbackMessage;

// @Component
public class FeedbackFiveStream {

    private static final Set<String> GOOD_WORDS = Set.of("happy", "good", "helpful");
    private static final Set<String> BAD_WORDS = Set.of("angry", "sad", "bad");

    @Autowired
    void kstreamFeedback(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var feedbackSerde = new JsonSerde<>(FeedbackMessage.class);

        builder.stream("t-commodity-feedback", Consumed.with(stringSerde, feedbackSerde))
                .flatMap(splitWords())
                .split()
                .branch(isGoodWord(),
                        Branched.withConsumer(
                                ks -> ks.repartition(Repartitioned.as("t-commodity-feedback-five-good"))
                                        .groupByKey().count().toStream().to("t-commodity-feedback-five-good-count")))
                .branch(isBadWord(),
                        Branched.withConsumer(
                                ks -> ks.repartition(Repartitioned.as("t-commodity-feedback-five-bad"))
                                        .groupByKey().count().toStream().to("t-commodity-feedback-five-bad-count")));

        // .branch(isGoodWord(), isBadWord());

        // feedbackStreams[0].through("t-commodity-feedback-five-good")
        // .groupByKey().count().toStream().to("t-commodity-feedback-five-good-count");

        // feedbackStreams[1].through("t-commodity-feedback-five-bad")
        // .groupByKey().count().toStream().to("t-commodity-feedback-five-bad-count");
    }

    private Predicate<String, String> isBadWord() {
        return (key, value) -> BAD_WORDS.contains(value);
    }

    private Predicate<String, String> isGoodWord() {
        return (key, value) -> GOOD_WORDS.contains(value);
    }

    private KeyValueMapper<String, FeedbackMessage, Iterable<KeyValue<String, String>>> splitWords() {
        return (key, value) -> Arrays
                .asList(value.getFeedback().replaceAll("[^a-zA-Z]", " ")
                        .toLowerCase().split("\\s+"))
                .stream()
                .distinct()
                .map(word -> KeyValue.pair(value.getLocation(), word)).collect(Collectors.toList());
    }

}
