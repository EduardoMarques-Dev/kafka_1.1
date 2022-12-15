package com.cadu.cadufka.domain.model;

import java.math.BigDecimal;

public class Order {

    private final String userId, oderId;
    private final BigDecimal amount;

    public Order(String userId, String oderId, BigDecimal amount) {
        this.userId = userId;
        this.oderId = oderId;
        this.amount = amount;
    }
}
