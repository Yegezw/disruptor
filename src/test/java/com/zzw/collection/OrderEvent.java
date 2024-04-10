package com.zzw.collection;

import lombok.Data;

/**
 * 订单事件
 */
@Data
public class OrderEvent {

    private int price;
    private String message;
}
