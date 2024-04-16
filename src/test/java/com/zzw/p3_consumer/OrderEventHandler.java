package com.zzw.p3_consumer;

import com.zzw.p1_producer.OrderEvent;

/**
 * 订单事件处理器
 */
public class OrderEventHandler implements EventHandler<OrderEvent>
{

    @Override
    public void onEvent(OrderEvent event, long sequence, boolean endOfBatch) {
        System.out.println("消费者消费事件: " + event + " sequence = " + sequence + " endOfBatch = " + endOfBatch);
    }
}
