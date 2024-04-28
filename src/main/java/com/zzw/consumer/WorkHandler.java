package com.zzw.consumer;

/**
 * 事件处理器接口(多线程消费者)
 */
public interface WorkHandler<T>
{

    /**
     * 消费者消费事件
     *
     * @param event 事件对象
     */
    void consume(T event);
}