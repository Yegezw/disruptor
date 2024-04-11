package com.zzw.consumer;

import com.zzw.collection.OrderEvent;

/**
 * 订单事件处理器(多线程消费者)
 */
public class OrderWorkHandler implements WorkHandler<OrderEvent> {

    private String consumerName;

    public OrderWorkHandler(String consumerName) {
        this.consumerName = consumerName;
    }

    @Override
    public void consume(OrderEvent event) {
        System.out.println(consumerName + " 消费者消费事件: " + event);
    }
}
