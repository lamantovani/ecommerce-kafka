package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailService {

    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService<>(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-------------------------------------------------");
        System.out.println("Send email");
        System.out.println("KEY: " + record.key());
        System.out.println("VALUE: " + record.value());
        System.out.println("PARTITION: " + record.partition());
        System.out.println("OFFSET: " + record.offset());
        System.out.println("-------------------------------------------------");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
