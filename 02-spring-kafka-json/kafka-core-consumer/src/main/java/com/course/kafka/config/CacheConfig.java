package com.course.kafka.config;

import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

@Configuration
public class CacheConfig {

    @Bean(name = "cachePurchaseRequest")
    Cache<String, Boolean> cachePurchaseRequest() {
        return Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(1000).build();
    }

    @Bean(name = "cachePaymentRequest")
    Cache<String, Boolean> cachePaymentRequest() {
        return Caffeine.newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).maximumSize(1000).build();
    }

}
