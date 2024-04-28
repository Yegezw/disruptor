package com.zzw.collection.dsl.consumer;

import com.zzw.consumer.EventProcessor;
import com.zzw.consumer.WorkerPool;
import com.zzw.relation.Sequence;

import java.util.*;

/**
 * 维护当前 disruptor 的所有消费者信息的仓库
 */
public class ConsumerRepository<T>
{

    /**
     * 消费者信息集合
     */
    private final List<ConsumerInfo>          consumerInfos                = new ArrayList<>();
    /**
     * 不重写 Sequence 的 hashCode、equals, 因为比对的就是原始对象是否相等
     */
    private final Map<Sequence, ConsumerInfo> eventProcessorInfoBySequence = new IdentityHashMap<>();

    public List<ConsumerInfo> getConsumerInfos()
    {
        return consumerInfos;
    }

    public void add(final EventProcessor processor)
    {
        final EventProcessorInfo<T> consumerInfo = new EventProcessorInfo<>(processor);
        eventProcessorInfoBySequence.put(processor.getCurrentConsumeSequence(), consumerInfo);
        consumerInfos.add(consumerInfo);
    }

    public void add(final WorkerPool<T> workerPool)
    {
        final WorkerPoolInfo<T> workerPoolInfo = new WorkerPoolInfo<>(workerPool);
        for (Sequence sequence : workerPool.getCurrentWorkerSequences())
        {
            eventProcessorInfoBySequence.put(sequence, workerPoolInfo);
        }
        consumerInfos.add(workerPoolInfo);
    }

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

    public void unMarkEventProcessorsAsEndOfChain(final Sequence... barrierEventProcessors)
    {
        for (Sequence barrierEventProcessor : barrierEventProcessors)
        {
            eventProcessorInfoBySequence.get(barrierEventProcessor).markAsUsedInBarrier();
        }
    }
}
