package com.zzw.consumer;

import com.zzw.collection.OrderEvent;

/**
 * 订单事件处理器
 */
public class OrderEventHandler implements EventHandler<OrderEvent> {

    private String consumerName;

    public OrderEventHandler(String consumerName) {
        this.consumerName = consumerName;
    }

    /**
     * 消费者消费事件
     *
     * @param event      事件对象
     * @param sequence   事件对象在 RingBuffer 里的序号
     * @param endOfBatch 当前事件是否是这一批事件中的最后一个
     */
    @Override
    public void consume(OrderEvent event, long sequence, boolean endOfBatch) {
        System.out.println(consumerName + " 消费者消费事件: " + event + " sequence = " + sequence + " endOfBatch = " + endOfBatch);
    }
}
