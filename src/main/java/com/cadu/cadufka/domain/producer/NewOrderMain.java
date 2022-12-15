package com.cadu.cadufka.domain.producer;

import com.cadu.cadufka.domain.model.Order;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()){
            try(var emailDispatcher = new KafkaDispatcher<String>()){
                for (var i=0; i<10; i++){
                    //Key: responsável por organizar as mensagens em partições
                    var userId = UUID.randomUUID().toString();

                    //Conteúdo da order
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var orderValue = new Order(userId, orderId, amount);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER",userId,orderValue);

                    //Conteúdo do email
                    var emailValue = "Thank you for your order! We are processing your order!";
                    emailDispatcher.send("ECOMMERCE_NEW_ORDER",userId,emailValue);
                }
            }
        }
    }


}
