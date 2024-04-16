package com.zzw.p1_producer;

/**
 * 订单事件工厂
 */
public class OrderEventFactory implements EventFactory<OrderEvent>
{

    @Override
    public OrderEvent newInstance()
    {
        return new OrderEvent();
    }
}
