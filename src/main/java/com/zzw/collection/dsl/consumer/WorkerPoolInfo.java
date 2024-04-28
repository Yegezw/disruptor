package com.zzw.collection.dsl.consumer;

import com.zzw.consumer.WorkerPool;
import com.zzw.relation.Sequence;

import java.util.concurrent.Executor;

/**
 * 多线程事件处理器(消费者)信息
 */
public class WorkerPoolInfo<T> implements ConsumerInfo
{

    /**
     * 多线程消费者池
     */
    private final WorkerPool<T> workerPool;
    /**
     * 默认 "是最尾端的消费者"
     */
    private       boolean       endOfChain = true;

    public WorkerPoolInfo(WorkerPool<T> workerPool)
    {
        this.workerPool = workerPool;
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
        return workerPool.getCurrentWorkerSequences();
    }
}
