package com.zzw.collection.dsl.consumer;

import com.zzw.consumer.WorkerPool;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;

import java.util.concurrent.Executor;

/**
 * 多线程事件处理器(消费者)信息
 */
public class WorkerPoolInfo<T> implements ConsumerInfo
{

    /**
     * 多线程消费者池
     */
    private final WorkerPool<T>   workerPool;
    /**
     * 消费者的消费序号屏障
     */
    private final SequenceBarrier sequenceBarrier;
    /**
     * 默认 "是最尾端的消费者"
     */
    private       boolean         endOfChain = true;

    public WorkerPoolInfo(final WorkerPool<T> workerPool, final SequenceBarrier sequenceBarrier)
    {
        this.workerPool      = workerPool;
        this.sequenceBarrier = sequenceBarrier;
    }

    @Override
    public void start(Executor executor)
    {
        workerPool.start(executor);
    }

    @Override
    public void halt()
    {
        workerPool.halt();
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
        return workerPool.isRunning();
    }

    @Override
    public Sequence[] getSequences()
    {
        return workerPool.getWorkerSequences();
    }

    @Override
    public SequenceBarrier getBarrier()
    {
        return sequenceBarrier;
    }
}
