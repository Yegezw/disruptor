package com.zzw.collection.dsl.consumer;

import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;

import java.util.concurrent.Executor;

/**
 * 消费者信息
 */
public interface ConsumerInfo
{

    /**
     * 通过 executor 启动当前消费者线程
     *
     * @param executor 执行器
     */
    void start(Executor executor);

    /**
     * 停止当前消费者线程
     */
    void halt();

    /**
     * 是否为 "最尾端的消费者"
     */
    boolean isEndOfChain();

    /**
     * 将当前消费者标记为 "非最尾端的消费者"
     */
    void markAsUsedInBarrier();

    /**
     * 当前消费者线程是否在运行中
     */
    boolean isRunning();

    /**
     * 获得消费者的序号(多线程消费者有多个序号对象)
     */
    Sequence[] getSequences();

    /**
     * 获得消费者的消费序号屏障
     */
    SequenceBarrier getBarrier();
}
