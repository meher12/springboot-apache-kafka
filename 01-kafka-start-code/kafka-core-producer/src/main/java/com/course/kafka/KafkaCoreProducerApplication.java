package com.course.kafka;

import com.course.kafka.producer.KafkaKeyProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//@EnableScheduling
public class KafkaCoreProducerApplication implements CommandLineRunner {

    //@Autowired
    //private HelloKafkaProducer helloKafkaProducer;

    /*@Autowired
    private FixeRateProducer fixeRateProducer;*/

    @Autowired
    private KafkaKeyProducer kafkaKeyProducer;

    public static void main(String[] args) {
        SpringApplication.run(KafkaCoreProducerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        //helloKafkaProducer.sendHello("Maher " + ThreadLocalRandom.current().nextInt(1000));
        // Simple for loop to send messages
        for (int i = 0; i < 1000; i++) {
            String key = "Key-" + i; // Create a unique key for each message
            String value = "Message-" + i; // Create a value for each message
            kafkaKeyProducer.sendMessage(key,value);
        }
    }
}