package com.course.kafka;

import com.course.kafka.entity.Employee;
import com.course.kafka.entity.PaymentRequest;
import com.course.kafka.entity.PurchaseRequest;
import com.course.kafka.producer.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.LocalDate;
import java.util.UUID;

@SpringBootApplication
//@EnableScheduling
public class KafkaJsonProducerApplication implements CommandLineRunner {

//    @Autowired
//    private Employee2JsonProducer employeeJsonProducer;

//    @Autowired
//    private CounterProducer counterProducer;

//    @Autowired
//    private PurchaseRequestProducer purchaseRequestProducer;

    @Autowired
    private PaymentRequestProducer paymentRequestProducer;

    public static void main(String[] args) {
        SpringApplication.run(KafkaJsonProducerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
       /* for (int i = 0; i < 5; i++) {
            Employee employee = new Employee(UUID.randomUUID().toString(),
                    "Employee" + i,
                    LocalDate.now().minusYears(20 + i));
            employeeJsonProducer.sendMessage(employee);
        }*/

        //counterProducer.sendMessages(100);

       /* PurchaseRequest request1 = new PurchaseRequest(UUID.randomUUID(), "REQ-001", 100, "USD");
        PurchaseRequest request2 = new PurchaseRequest(UUID.randomUUID(), "REQ-002", 200, "EUR");
        PurchaseRequest request3 = new PurchaseRequest(UUID.randomUUID(), "REQ-003", 300, "GBP");

        purchaseRequestProducer.sendPurchaseRequest(request1);
        purchaseRequestProducer.sendPurchaseRequest(request2);
        purchaseRequestProducer.sendPurchaseRequest(request3);

        purchaseRequestProducer.sendPurchaseRequest(request1);*/

        PaymentRequest payment1 = new PaymentRequest(500, "USD", "123456789", "Monthly subscription", LocalDate.of(2024, 1, 10));
        PaymentRequest payment2 = new PaymentRequest(1000, "EUR", "987654321", "Invoice #456", LocalDate.of(2024, 2, 15));
        PaymentRequest payment3 = new PaymentRequest(250, "GBP", "567890123", "Gift payment", LocalDate.of(2024, 3, 20));
        PaymentRequest payment4 = new PaymentRequest(750, "JPY", "432109876", "Service fee", LocalDate.of(2024, 4, 5));
        PaymentRequest payment5 = new PaymentRequest(300, "CAD", "345678901", "Refund", LocalDate.of(2024, 5, 12));
        PaymentRequest payment6 = new PaymentRequest(1200, "AUD", "210987654", "Project milestone payment", LocalDate.of(2024, 6, 25));

        paymentRequestProducer.sendPaymentRequest(payment1);
        paymentRequestProducer.sendPaymentRequest(payment2);
        paymentRequestProducer.sendPaymentRequest(payment3);
        paymentRequestProducer.sendPaymentRequest(payment4);
        paymentRequestProducer.sendPaymentRequest(payment5);
        paymentRequestProducer.sendPaymentRequest(payment6);

        //Publish two of them agin:
        paymentRequestProducer.sendPaymentRequest(payment1);
        paymentRequestProducer.sendPaymentRequest(payment2);


    }
}