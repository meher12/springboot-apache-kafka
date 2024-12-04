package com.course.kafka.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.course.kafka.entity.CarLocation;
import com.course.kafka.producer.CarLocationProducer;

//@Service
public class CarLocationScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(CarLocationScheduler.class);

    private CarLocation carLocationOne;
    private CarLocation carLocationTwo;
    private CarLocation carLocationThree;

    @Autowired
    private CarLocationProducer producer;

    public CarLocationScheduler() {
        var now = System.currentTimeMillis();

        carLocationOne = new CarLocation("car-one", now, 0);
        carLocationTwo = new CarLocation("car-two", now, 110);
        carLocationThree = new CarLocation("car-three", now, 95);
    }

    @Scheduled(fixedRate = 10000)
    public void generateDummyData() {
        var now = System.currentTimeMillis();
        carLocationOne.setTimestamp(now);
        carLocationTwo.setTimestamp(now);
        carLocationThree.setTimestamp(now);

        carLocationOne.setDistance(carLocationOne.getDistance() + 1);
        carLocationTwo.setDistance(carLocationTwo.getDistance() - 1);
        carLocationThree.setDistance(carLocationThree.getDistance() + 1);

        sendCarLocation(carLocationOne);
        sendCarLocation(carLocationTwo);
        sendCarLocation(carLocationThree);
    }

    private void sendCarLocation(CarLocation carLocation) {
        producer.sendCarLocation(carLocation);
        LOG.info("Sent car location: {}", carLocation);
    }
}