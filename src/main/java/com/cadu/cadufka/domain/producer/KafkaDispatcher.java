package com.cadu.cadufka.domain.producer;

import com.cadu.cadufka.domain.util.GsonSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher<T>  implements Closeable {

    private final KafkaProducer<String, T> producer;

    KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    private static Properties properties(){
        var properties = new Properties();

        // Onde a gente vai se conectar? Onde está rodando os KAFKA'S
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");

        // Transformadores/Serializadores de Strings para Bytes
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());

        return properties;
    }

    void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        // A mensagem é um "record", pq é um registro q ficará armazenado no KAFKA
        var record = new ProducerRecord<>(topic,key,value);
        producer.send(record, getCallback()).get();
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

    @Override
    public void close() {
        producer.close();
    }
}
