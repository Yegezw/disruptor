package com.zzw.collection.dsl.consumer;

import com.zzw.consumer.EventProcessor;
import com.zzw.consumer.WorkerPool;

import java.util.ArrayList;
import java.util.List;

/**
 * 维护当前 disruptor 的所有消费者信息的仓库
 */
public class ConsumerRepository<T> {

    /**
     * 消费者信息集合
     */
    private final List<ConsumerInfo> consumerInfos = new ArrayList<>();

    public List<ConsumerInfo> getConsumerInfos() {
        return consumerInfos;
    }

    public void add(final EventProcessor processor) {
        final EventProcessorInfo<T> consumerInfo = new EventProcessorInfo<>(processor);
        consumerInfos.add(consumerInfo);
    }

    public void add(final WorkerPool<T> workerPool) {
        final WorkerPoolInfo<T> workerPoolInfo = new WorkerPoolInfo<>(workerPool);
        consumerInfos.add(workerPoolInfo);
    }
}
