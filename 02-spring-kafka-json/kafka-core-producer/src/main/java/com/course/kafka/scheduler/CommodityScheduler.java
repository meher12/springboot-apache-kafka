package com.course.kafka.scheduler;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.course.kafka.entity.Commodity;
import com.course.kafka.producer.CommodityProducer;

//@Component
public class CommodityScheduler {

    private static final String COMMODITY_API_URL = "http://localhost:8080/api/commodity/v1/all";

    @Autowired
    private CommodityProducer producer;

    private RestTemplate restTemplate = new RestTemplate();

    @Scheduled(fixedRate = 5000)
    public void fetchAndSendCommodities() {
        Commodity[] commodities = restTemplate.getForObject(COMMODITY_API_URL,
                Commodity[].class);
        if (commodities != null) {
            Arrays.stream(commodities).forEach(producer::sendMessage);
        }
    }

}