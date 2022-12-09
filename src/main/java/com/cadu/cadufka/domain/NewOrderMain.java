package com.cadu.cadufka.domain;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Responsável por enviar a mensagem.
        var producer = new KafkaProducer<String, String>(properties());
        var value = "id_Pedido,id_Usuario,id_Compra";
        // A mensagem é um "record", pq é um registro q ficará armazenado no KAFKA
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", value, value);

        producer.send(record, (data, ex) -> {
            if (ex != null){
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviado - "+data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        }).get();
    }

    private static Properties properties(){
        var properties = new Properties();

        // Onde a gente vai se conectar? Onde está rodando os KAFKA'S
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        // Transformadores/Serializadores de Strings para Bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
