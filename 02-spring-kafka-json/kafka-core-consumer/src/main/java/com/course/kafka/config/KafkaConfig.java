package com.course.kafka.config;

import com.course.kafka.entity.CarLocation;
import com.course.kafka.entity.PaymentRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import com.github.benmanes.caffeine.cache.Cache;


@Configuration
public class KafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    ConsumerFactory<Object, Object> consumerFactory(SslBundles sslBundles) {
        var properties = kafkaProperties.buildConsumerProperties(sslBundles);

//        properties.put(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "40000");

        return new DefaultKafkaConsumerFactory<>(properties);
    }

    @Bean(name = "locationFarContainerFactory")
    ConcurrentKafkaListenerContainerFactory<Object, Object> locationFarContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            SslBundles sslBundles,
            ObjectMapper objectMapper) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory(sslBundles));

        factory.setRecordFilterStrategy(new RecordFilterStrategy<Object, Object>() {

            @Override
            public boolean filter(@SuppressWarnings("null") ConsumerRecord<Object, Object> consumerRecord) {
                try {
                    var carLocation = objectMapper.readValue(consumerRecord.value().toString(), CarLocation.class);

                    return carLocation.getDistance() <= 100;
                } catch (Exception e) {
                    return false;
                }
            }
        });

        return factory;
    }

    @Bean(name = "paymentRequestContainerFactory")
    ConcurrentKafkaListenerContainerFactory<Object, Object> paymentRequestContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            SslBundles sslBundles,
            ObjectMapper objectMapper,
            @Qualifier("cachePaymentRequest") Cache<String, Boolean> cachePaymentRequest) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        configurer.configure(factory, consumerFactory(sslBundles));

        factory.setRecordFilterStrategy(new RecordFilterStrategy<Object, Object>() {

            @Override
            public boolean filter(@SuppressWarnings("null") ConsumerRecord<Object, Object> consumerRecord) {
                try {
                    var paymentRequest = objectMapper.readValue(consumerRecord.value().toString(),
                            PaymentRequest.class);
                    var cacheKey = paymentRequest.calculateHash();

                    return cachePaymentRequest.getIfPresent(cacheKey) != null;
                } catch (Exception e) {
                    return false;
                }
            }
        });

        return factory;
    }

}
