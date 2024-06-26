package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var logService = new LogService();
        try (var service = new KafkaService<>(LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                String.class,
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("-------------------------------------------------");
        System.out.println("LOG ==> " + " TOPIC: " + record.topic() + "KEY: " + record.key() + " VALUE: " + record.value() + " PARTITION: " + record.partition() + "OFFSET: " + record.offset());
        System.out.println("-------------------------------------------------");
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
