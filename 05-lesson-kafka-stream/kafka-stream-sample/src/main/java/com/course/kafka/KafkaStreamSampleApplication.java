package com.course.kafka;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamSampleApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamSampleApplication.class, args);
	}

	// @Autowired
	// private OperationDemoStream operationDemoStream;

	@Override
	public void run(String... args) throws Exception {
	}

}
