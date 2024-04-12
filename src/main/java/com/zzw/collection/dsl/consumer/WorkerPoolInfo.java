package com.zzw.collection.dsl.consumer;

import com.zzw.consumer.WorkerPool;

import java.util.concurrent.Executor;

/**
 * 多线程事件处理器(消费者)信息
 */
public class WorkerPoolInfo<T> implements ConsumerInfo {

    /**
     * 多线程消费者池
     */
    private final WorkerPool<T> workerPool;

    public WorkerPoolInfo(WorkerPool<T> workerPool) {
        this.workerPool = workerPool;
    }

    @Override
    public void start(Executor executor) {
        workerPool.start(executor);
    }
}
