package com.cadu.cadufka.domain;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Responsável por receber a mensagem.
        var consumer = new KafkaConsumer<String, String>(properties());

        // Informo o tópico que quero escutar
        consumer.subscribe(Collections.singleton("ECOMMERCE_NEW_ORDER"));

        // Cria laço infinito para escuta contínua
        while (true){
            // Pergunta se tem mensagem por um *TEMPO*, o que vai devolver vários registros
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()){
                System.out.println("Encontrei "+ records.count() +" registros");
                for (var record : records){
                    System.out.println("-----------------------------------------");
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println("Chave: "+record.key());
                    System.out.println("Valor: "+record.value());
                    System.out.println("Partição: "+record.partition());
                    System.out.println("Offset: "+record.offset());
                    try{
                        Thread.sleep(5000);
                    }catch (InterruptedException ex){
                        //ignoring
                        ex.printStackTrace();
                    }
                    System.out.println("Order processed");
                }
            }

        }
    }

    private static Properties properties(){
        var properties = new Properties();

        // Onde a gente vai se conectar? Onde está rodando os KAFKA'S
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        // Transformadores/Deserializadores de Strings para Bytes
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getName());

        return properties;
    }
}
