package com.zzw.collection;

/**
 * 订单事件工厂
 */
public class OrderEventFactory implements EventFactory<OrderEvent>
{

    /**
     * 创建一个裸订单事件, 用于填充 RingBuffer
     */
    @Override
    public OrderEvent newInstance()
    {
        return new OrderEvent();
    }
}
