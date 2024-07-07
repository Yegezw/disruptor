package com.zzw.collection.dsl.consumer;

import com.zzw.consumer.EventHandler;
import com.zzw.consumer.EventProcessor;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;

import java.util.concurrent.Executor;

/**
 * 单线程事件处理器(消费者)信息
 */
public class EventProcessorInfo<T> implements ConsumerInfo
{

    /**
     * 事件处理器接口(消费者线程)
     */
    private final EventProcessor          eventProcessor;
    /**
     * 事件处理器接口(消费者接口)
     */
    private final EventHandler<? super T> handler;
    /**
     * 消费者的消费序号屏障
     */
    private final SequenceBarrier         sequenceBarrier;
    /**
     * 默认 "是最尾端的消费者"
     */
    private       boolean                 endOfChain = true;

    public EventProcessorInfo(final EventProcessor eventProcessor,
                              final EventHandler<? super T> handler,
                              final SequenceBarrier sequenceBarrier)
    {
        this.eventProcessor  = eventProcessor;
        this.handler         = handler;
        this.sequenceBarrier = sequenceBarrier;
    }

    public EventProcessor getEventProcessor()
    {
        return eventProcessor;
    }

    public EventHandler<? super T> getHandler()
    {
        return handler;
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
        return new Sequence[]{eventProcessor.getSequence()};
    }

    @Override
    public SequenceBarrier getBarrier()
    {
        return sequenceBarrier;
    }
}
