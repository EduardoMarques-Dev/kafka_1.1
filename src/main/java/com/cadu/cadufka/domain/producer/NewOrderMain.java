package com.cadu.cadufka.domain.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Responsável por enviar a mensagem.
        var producer = new KafkaProducer<String, String>(properties());

        for (var i=0; i<10; i++){
            //Key: responsável por organizar as mensagens em partições
            var key = UUID.randomUUID().toString();

            // A mensagem é um "record", pq é um registro q ficará armazenado no KAFKA
            var orderValue = key+",id_Pedido,id_Compra";
            var orderRecord = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER",key, orderValue);

            var emailValue = key+", Thank you for your order! We are processing your order!";
            var emailRecord = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL",key,emailValue);

            producer.send(orderRecord, getCallback()).get();
            producer.send(emailRecord, getCallback()).get();
        }
    }

    private static Callback getCallback() {
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviado - " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
        return callback;
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
