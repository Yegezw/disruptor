package com.zzw.collection.dsl.consumer;

import com.zzw.consumer.EventProcessor;
import com.zzw.relation.Sequence;

import java.util.concurrent.Executor;

/**
 * 单线程事件处理器(消费者)信息
 */
public class EventProcessorInfo<T> implements ConsumerInfo
{

    /**
     * 事件处理器接口(消费者)
     */
    private final EventProcessor eventProcessor;
    /**
     * 默认 "是最尾端的消费者"
     */
    private       boolean        endOfChain = true;

    public EventProcessorInfo(EventProcessor eventProcessor)
    {
        this.eventProcessor = eventProcessor;
    }

    @Override
    public void start(Executor executor)
    {
        executor.execute(eventProcessor);
    }

    @Override
    public void halt()
    {
        eventProcessor.halt();
    }

    @Override
    public boolean isEndOfChain()
    {
        return endOfChain;
    }

    @Override
    public void markAsUsedInBarrier()
    {
        endOfChain = false;
    }

    @Override
    public boolean isRunning()
    {
        return eventProcessor.isRunning();
    }

    @Override
    public Sequence[] getSequences()
    {
        return new Sequence[]{eventProcessor.getCurrentConsumeSequence()};
    }
}
