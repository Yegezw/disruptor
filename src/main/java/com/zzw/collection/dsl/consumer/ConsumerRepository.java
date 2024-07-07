package com.zzw.collection.dsl.consumer;

import com.zzw.consumer.EventHandler;
import com.zzw.consumer.EventProcessor;
import com.zzw.consumer.WorkerPool;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;

import java.util.*;

/**
 * 维护当前 disruptor 的所有消费者信息的仓库
 */
public class ConsumerRepository<T> implements Iterable<ConsumerInfo>
{

    /**
     * 消费者信息集合
     */
    private final List<ConsumerInfo> consumerInfos = new ArrayList<>();

    /**
     * 不重写 Sequence 的 hashCode、equals, 因为比对的就是原始对象是否相等
     */
    private final Map<Sequence, ConsumerInfo> consumerInfoBySequence = new IdentityHashMap<>();

    /**
     * 不重写 EventHandler 的 hashCode、equals, 因为比对的就是原始对象是否相等
     */
    private final Map<EventHandler<?>, EventProcessorInfo<T>> eventProcessorInfoByEventHandler = new IdentityHashMap<>();

    // =============================================================================

    public List<ConsumerInfo> getConsumerInfos()
    {
        return consumerInfos;
    }

    /**
     * 针对 EventProcessor(BatchEventProcessor、WorkProcessor)
     */
    public void add(final EventProcessor processor)
    {
        final EventProcessorInfo<T> consumerInfo = new EventProcessorInfo<>(processor, null, null);
        consumerInfos.add(consumerInfo);
        consumerInfoBySequence.put(processor.getSequence(), consumerInfo);
    }

    /**
     * 针对 BatchEventProcessor
     */
    public void add(
            final EventProcessor processor,
            final EventHandler<? super T> handler,
            final SequenceBarrier barrier)
    {
        final EventProcessorInfo<T> consumerInfo = new EventProcessorInfo<>(processor, handler, barrier);
        consumerInfos.add(consumerInfo);
        consumerInfoBySequence.put(processor.getSequence(), consumerInfo);
        eventProcessorInfoByEventHandler.put(handler, consumerInfo);
    }

    /**
     * 针对 WorkerPool
     */
    public void add(final WorkerPool<T> workerPool, final SequenceBarrier barrier)
    {
        final WorkerPoolInfo<T> workerPoolInfo = new WorkerPoolInfo<>(workerPool, barrier);
        consumerInfos.add(workerPoolInfo);
        for (Sequence sequence : workerPool.getWorkerSequences())
        {
            consumerInfoBySequence.put(sequence, workerPoolInfo);
        }
    }

    // ----------------------------------------

    /**
     * EventHandler -> EventProcessorInfo -> EventProcessor
     */
    public EventProcessor getEventProcessorFor(final EventHandler<T> handler)
    {
        final EventProcessorInfo<T> eventProcessorInfo = getEventProcessorInfo(handler);
        if (eventProcessorInfo == null)
        {
            throw new IllegalArgumentException("The event handler " + handler + " is not processing events.");
        }

        return eventProcessorInfo.getEventProcessor();
    }

    /**
     * EventHandler -> EventProcessor -> Sequence
     */
    public Sequence getSequenceFor(final EventHandler<T> handler)
    {
        return getEventProcessorFor(handler).getSequence();
    }

    /**
     * EventHandler -> EventProcessorInfo -> SequenceBarrier
     */
    public SequenceBarrier getBarrierFor(final EventHandler<T> handler)
    {
        final ConsumerInfo consumerInfo = getEventProcessorInfo(handler);
        return consumerInfo != null ? consumerInfo.getBarrier() : null;
    }

    // ----------------------------------------

    /**
     * 找到所有 "运行中 + 处于最尾端" 的消费序号集合
     */
    public List<Sequence> getLastSequenceInChain()
    {
        List<Sequence> lastSequenceList = new ArrayList<>();

        for (ConsumerInfo consumerInfo : consumerInfos)
        {
            if (consumerInfo.isRunning() && consumerInfo.isEndOfChain())
            {
                final Sequence[] sequences = consumerInfo.getSequences();
                Collections.addAll(lastSequenceList, sequences);
            }
        }

        return lastSequenceList;
    }

    /**
     * sequence in sequences[] -> ConsumerInfo.endOfChain = false
     */
    public void unMarkEventProcessorsAsEndOfChain(final Sequence... sequences)
    {
        for (Sequence sequence : sequences)
        {
            getConsumerInfo(sequence).markAsUsedInBarrier();
        }
    }

    // =============================================================================

    @Override
    public Iterator<ConsumerInfo> iterator()
    {
        return consumerInfos.iterator();
    }

    /**
     * EventHandler -> EventProcessorInfo
     */
    private EventProcessorInfo<T> getEventProcessorInfo(final EventHandler<T> handler)
    {
        return eventProcessorInfoByEventHandler.get(handler);
    }

    /**
     * Sequence -> ConsumerInfo
     */
    private ConsumerInfo getConsumerInfo(final Sequence sequence)
    {
        return consumerInfoBySequence.get(sequence);
    }
}
