package com.course.kafka.scheduler;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.course.kafka.producer.GeneralLedgerProducer;

@Service
public class GeneralLedgerScheduler {

    private static final AtomicInteger COUNTER = new AtomicInteger();

    @Autowired
    private GeneralLedgerProducer producer;

    @Scheduled(fixedRate = 1000)
    public void schedule() {
        producer.sendGeneralLedgerMessage("Message " + COUNTER.getAndIncrement());
    }

}
