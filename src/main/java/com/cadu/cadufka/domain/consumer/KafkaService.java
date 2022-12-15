package com.cadu.cadufka.domain.consumer;

import com.cadu.cadufka.domain.ConsumerFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaService implements Closeable {
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction parse){
        // Responsável por receber a mensagem.
        this.consumer = new KafkaConsumer<>(properties(groupId));
        this.parse = parse;
        // Informo o tópico que quero escutar
        consumer.subscribe(Collections.singleton(topic));
    }

    public void run(){
        // Cria laço infinito para escuta contínua
        while (true){
            // Pergunta se tem mensagem por um *TEMPO*, o que vai devolver vários registros
            var records = consumer.poll(Duration.ofMillis(100));
            // Se tiver registros:
            if (!records.isEmpty()){
                System.out.println("Encontrei "+ records.count() +" registros!");
                for (var record : records){
                    parse.consume(record);
                }
            }
        }
    }

    static Properties properties(String groupId){
        var properties = new Properties();
        // Onde a gente vai se conectar? Onde está rodando os KAFKA'S
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        // Transformadores/Deserializadores de Strings para Bytes
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Define o grupo do consumidor, o qual vários podem participar
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Define o nome específico do consumidor, e em um grupo, cada um tem o seu
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        // Define o máximo de mensagens trabalhadas por vez
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1");
        return properties;
    }

    @Override
    public void close(){
        consumer.close();
    }
}
