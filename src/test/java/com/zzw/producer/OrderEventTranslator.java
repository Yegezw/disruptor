package com.zzw.producer;

import com.zzw.collection.OrderEvent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 订单事件更新器
 */
public class OrderEventTranslator implements EventTranslatorVararg<OrderEvent>
{

    private final AtomicInteger count = new AtomicInteger(0);

    @Override
    public void translateTo(OrderEvent event, long sequence, Object... args)
    {
        int i = count.getAndIncrement();
        event.setMessage("message-" + i);
        event.setPrice(i * 10);

        System.out.println(Thread.currentThread().getName() + " 生产者更新 " + sequence + " 号事件: " + event);
    }
}
