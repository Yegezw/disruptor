package com.zzw.consumer;

/**
 * 事件处理器接口
 */
public interface EventHandler<T>
{

    /**
     * 消费者消费事件
     *
     * @param event      事件对象
     * @param sequence   事件对象在 RingBuffer 里的序号
     * @param endOfBatch 当前事件是否是这一批事件中的最后一个
     */
    void onEvent(T event, long sequence, boolean endOfBatch);

    /**
     * 获取消费者名称
     */
    String getName();
}
