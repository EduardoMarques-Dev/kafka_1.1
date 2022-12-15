package com.cadu.cadufka.domain.producer;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dispatcher = new KafkaDispatcher()){
            for (var i=0; i<10; i++){
                //Key: responsável por organizar as mensagens em partições
                var key = UUID.randomUUID().toString();

                // Conteúdo da mensagem
                var orderValue = key+",id_Pedido,id_Compra";
                var emailValue = key+", Thank you for your order! We are processing your order!";

                dispatcher.send("ECOMMERCE_NEW_ORDER",key,orderValue);
                dispatcher.send("ECOMMERCE_NEW_ORDER",key,emailValue);
            }
        }
    }


}
