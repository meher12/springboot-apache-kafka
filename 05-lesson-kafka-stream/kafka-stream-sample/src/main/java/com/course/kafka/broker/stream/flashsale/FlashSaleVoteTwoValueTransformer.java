package com.course.kafka.broker.stream.flashsale;

import java.time.OffsetDateTime;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import com.course.kafka.broker.message.FlashSaleVoteMessage;

public class FlashSaleVoteTwoValueTransformer implements ValueTransformer<FlashSaleVoteMessage, FlashSaleVoteMessage> {

    private final long voteStartTime;

    private final long voteEndTime;

    private ProcessorContext processorContext;

    public FlashSaleVoteTwoValueTransformer(OffsetDateTime startDateTime, OffsetDateTime endDateTime) {
        this.voteStartTime = startDateTime.toInstant().toEpochMilli();
        this.voteEndTime = endDateTime.toInstant().toEpochMilli();
    }

    @Override
    public void init(ProcessorContext context) {
        this.processorContext = context;
    }

    @Override
    public FlashSaleVoteMessage transform(FlashSaleVoteMessage value) {
        var recordTime = processorContext.timestamp();

        return (recordTime >= voteStartTime && recordTime <= voteEndTime) ? value : null;
    }

    @Override
    public void close() {
    }

}
