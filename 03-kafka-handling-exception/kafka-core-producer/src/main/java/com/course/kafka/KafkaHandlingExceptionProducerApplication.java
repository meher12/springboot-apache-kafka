package com.course.kafka;

import com.course.kafka.producer.Image2Producer;
import com.course.kafka.producer.InvoiceProducer;
import com.course.kafka.service.ImageService;
import com.course.kafka.service.InvoiceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaHandlingExceptionProducerApplication implements CommandLineRunner {

    /*@Autowired
    private FoodOrderProducer foodOrderProducer;

    @Autowired
    private SimpleNumberProducer simpleNumberProducer;*/

    /*@Autowired
    private ImageService imageService;

    @Autowired
    private ImageProducer imageProducer;*/
    
  /*  @Autowired
    private ImageService imageService;

    @Autowired
    private Image2Producer image2Producer;

    @Autowired
    private InvoiceService invoiceService;

    @Autowired
    private InvoiceProducer invoiceProducer;*/


    public static void main(String[] args) {
        SpringApplication.run(KafkaHandlingExceptionProducerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
       /* var chikenOrder = new FoodOrder(3, "chicken");
        var fishOrder = new FoodOrder(10, "fish");
        var pizzaOrder = new FoodOrder(5, "pizza");

        foodOrderProducer.sendFoodOrder(chikenOrder);
        foodOrderProducer.sendFoodOrder(fishOrder);
        foodOrderProducer.sendFoodOrder(pizzaOrder);
        for (int i = 0; i < 103; i++) {
            var simpleNumber = new SimpleNumber(i);
            simpleNumberProducer.sendSimpleNumber(simpleNumber);
        }*/


       /* var image1 = imageService.generateImage("jpg");
        var image2 = imageService.generateImage("svg");
        var image3 = imageService.generateImage("gif");
        var image4 = imageService.generateImage("bmp");
        var image5 = imageService.generateImage("tiff");
        var image6 = imageService.generateImage("png");

        imageProducer.sendImageToPartition(image1, 0);
        imageProducer.sendImageToPartition(image2, 0);
        imageProducer.sendImageToPartition(image3, 0);
        imageProducer.sendImageToPartition(image4, 1);
        imageProducer.sendImageToPartition(image5, 1);
        imageProducer.sendImageToPartition(image6, 1);*/
       /* for (int i = 0; i < 10; i++) {
            var invoice = invoiceService.generateInvoice();

            if( i> 5){
                invoice.setAmount(0d);
            }
            invoiceProducer.sendInvoice(invoice);
        }

        var image1 = imageService.generateImage("jpg");
        var image2 = imageService.generateImage("svg");
        var image3 = imageService.generateImage("gif");
        var image4 = imageService.generateImage("bmp");
        var image5 = imageService.generateImage("tiff");
        var image6 = imageService.generateImage("png");

        image2Producer.sendImageToPartition(image1, 0);
        image2Producer.sendImageToPartition(image2, 0);
        image2Producer.sendImageToPartition(image3, 0);
        image2Producer.sendImageToPartition(image4, 1);
        image2Producer.sendImageToPartition(image5, 1);
        image2Producer.sendImageToPartition(image6, 1);*/

    }
}