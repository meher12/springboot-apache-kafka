package com.course.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import com.course.kafka.broker.message.WebLayoutVoteMessage;

public class WebLayoutVoteTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var message = (WebLayoutVoteMessage) record.value();

        return message != null ? message.getVoteDateTime().toInstant().toEpochMilli()
                : record.timestamp();
    }

}
