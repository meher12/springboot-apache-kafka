package com.course.kafka.broker.stream.customer.preference;

import org.apache.kafka.streams.kstream.Aggregator;

import com.course.kafka.broker.message.CustomerPreferenceAggregateMessage;
import com.course.kafka.broker.message.CustomerPreferenceWishlistMessage;

public class CustomerPreferenceWishlistAggregator
        implements Aggregator<String, CustomerPreferenceWishlistMessage, CustomerPreferenceAggregateMessage> {

    @Override
    public CustomerPreferenceAggregateMessage apply(String key, CustomerPreferenceWishlistMessage value,
            CustomerPreferenceAggregateMessage aggregate) {
        aggregate.putWishlistItem(value.getItemName(), value.getWishlistDatetime());

        return aggregate;
    }

}
