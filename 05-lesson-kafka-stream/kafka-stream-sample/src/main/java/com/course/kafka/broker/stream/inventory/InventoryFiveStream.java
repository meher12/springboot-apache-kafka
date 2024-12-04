package com.course.kafka.broker.stream.inventory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.course.kafka.broker.message.InventoryMessage;
import com.course.kafka.util.InventoryTimestampExtractor;

// @Component
public class InventoryFiveStream {

    @Autowired
    void kstreamInventory(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var inventorySerde = new JsonSerde<>(InventoryMessage.class);
        var inventoryTimestampExtractor = new InventoryTimestampExtractor();
        var longSerde = Serdes.Long();
        var windowLength = Duration.ofHours(1l);
        var windowSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLength.toMillis());

        builder.stream("t-commodity-inventory",
                Consumed.with(stringSerde, inventorySerde, inventoryTimestampExtractor, null))
                .mapValues(
                        (k, v) -> v.getType().equalsIgnoreCase("ADD") ? v.getQuantity() : -1 * v.getQuantity())
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowLength))
                .reduce(Long::sum, Materialized.with(stringSerde, longSerde))
                .toStream()
                .peek(
                        (k, v) -> {
                            var windowStartTime = Instant.ofEpochMilli(k.window().start()).atOffset(ZoneOffset.UTC);
                            var windowEndTime = Instant.ofEpochMilli(k.window().end()).atOffset(ZoneOffset.UTC);

                            System.out.println("[" + k.key() + "@" + windowStartTime + "/" + windowEndTime + "], " + v);
                        })
                .to("t-commodity-inventory-five", Produced.with(windowSerde, longSerde));
    }

}
