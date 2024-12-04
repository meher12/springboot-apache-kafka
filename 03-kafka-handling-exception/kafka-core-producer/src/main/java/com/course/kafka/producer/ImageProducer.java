package com.course.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.course.kafka.entity.Image;
import com.fasterxml.jackson.databind.ObjectMapper;

//@Service
public class ImageProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    public void sendImageToPartition(Image image, int partition) {
        try {
            var imageJson = objectMapper.writeValueAsString(image);

            kafkaTemplate.send("t-image", partition, image.getType(), imageJson);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
