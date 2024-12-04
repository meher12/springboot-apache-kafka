package com.course.kafka.broker.stream.commodity;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderPatternMessage;
import com.course.kafka.broker.message.OrderRewardMessage;
import com.course.kafka.util.CommodityStreamUtil;

// @Component
public class CommodityFiveStream {

        private static final Logger LOG = LoggerFactory.getLogger(CommodityFiveStream.class);

        private void reportFraud(OrderMessage orderMessage) {
                LOG.info("Reporting fraud {}", orderMessage);
        }

        @Autowired
        void kstreamCommodityTrading(StreamsBuilder builder) {
                var orderSerde = new JsonSerde<>(OrderMessage.class);
                var orderPatternSerde = new JsonSerde<>(OrderPatternMessage.class);
                var orderRewardSerde = new JsonSerde<>(OrderRewardMessage.class);
                var stringSerde = Serdes.String();

                var maskedCreditCardStream = builder
                                .stream("t-commodity-order", Consumed.with(Serdes.String(), orderSerde))
                                .mapValues(CommodityStreamUtil::maskCreditCardNumber);

                maskedCreditCardStream.mapValues(CommodityStreamUtil::convertToOrderPatternMessage)
                                .split()
                                .branch(CommodityStreamUtil.isPlastic(),
                                                Branched.<String, OrderPatternMessage>withConsumer(
                                                                ks -> ks.to("t-commodity-pattern-five-plastic",
                                                                                Produced.with(stringSerde,
                                                                                                orderPatternSerde))))
                                .defaultBranch(
                                                Branched.<String, OrderPatternMessage>withConsumer(
                                                                ks -> ks.to("t-commodity-pattern-five-notplastic",
                                                                                Produced.with(stringSerde,
                                                                                                orderPatternSerde))));

                maskedCreditCardStream.filter(CommodityStreamUtil.isLargeQuantity())
                                .filterNot(CommodityStreamUtil.isCheap())
                                .map(CommodityStreamUtil.mapToOrderRewardChangeKey())
                                .to("t-commodity-reward-five", Produced.with(Serdes.String(), orderRewardSerde));

                maskedCreditCardStream
                                .selectKey(CommodityStreamUtil.generateStorageKey())
                                .to("t-commodity-storage-five", Produced.with(Serdes.String(), orderSerde));

                maskedCreditCardStream.filter(
                                (k, v) -> v.getOrderLocation().toUpperCase().startsWith("C")).peek(
                                                (k, v) -> reportFraud(v))
                                .map(
                                                (k, v) -> KeyValue.pair(
                                                                v.getOrderLocation().toUpperCase().charAt(0) + "***",
                                                                (long) (v.getPrice() * v.getQuantity())))
                                .to("t-commodity-fraud-five", Produced.with(stringSerde, Serdes.Long()));
        }

}