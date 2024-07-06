package com.zzw.producer;

import com.zzw.collection.OrderEvent;

/**
 * 订单事件更新器
 */
public class OrderEventTranslator implements EventTranslatorVararg<OrderEvent>
{

    private int num = 0;

    @Override
    public void translateTo(OrderEvent event, long sequence, Object... args)
    {
        event.setMessage("message-" + num);
        event.setPrice(num * 10);
        num++;

        System.out.println("生产者发布事件: " + event);
    }
}
