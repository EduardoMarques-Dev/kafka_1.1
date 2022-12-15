package com.cadu.cadufka.domain.consumer;

import com.cadu.cadufka.domain.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailService {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailService();
        var service = new KafkaService(EmailService.class.getSimpleName(),"ECOMMERCE_SEND_EMAIL", emailService::parse);
    }

    private void parse(ConsumerRecord<String,String> record){
        System.out.println("-----------------------------------------");
        System.out.println("Send email");
        System.out.println("Chave: "+record.key());
        System.out.println("Valor: "+record.value());
        System.out.println("Partição: "+record.partition());
        System.out.println("Offset: "+record.offset());
        try{
            Thread.sleep(1000);
        }catch (InterruptedException ex){
            //ignoring
            ex.printStackTrace();
        }
        System.out.println("Email sent");
    }
}
