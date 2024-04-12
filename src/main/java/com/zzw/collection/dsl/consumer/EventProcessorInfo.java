package com.zzw.collection.dsl.consumer;

import com.zzw.consumer.EventProcessor;

import java.util.concurrent.Executor;

/**
 * 单线程事件处理器(消费者)信息
 */
public class EventProcessorInfo<T> implements ConsumerInfo {

    /**
     * 事件处理器接口(消费者)
     */
    private final EventProcessor eventProcessor;

    public EventProcessorInfo(EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    @Override
    public void start(Executor executor) {
        executor.execute(eventProcessor);
    }
}
