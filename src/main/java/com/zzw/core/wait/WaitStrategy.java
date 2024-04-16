package com.zzw.core.wait;

import com.zzw.core.Sequence;
import com.zzw.core.consumer.barrier.SequenceBarrier;

/**
 * 消费者等待策略
 */
public interface WaitStrategy
{

    /**
     * 由消费者调用, 等待给定的序号可供使用
     *
     * @param sequence 申请消费的序号
     * @param cursor   生产序号
     * @return 最大可消费序号
     * @see SequenceBarrier#waitFor(long)
     */
    long waitFor(long sequence, Sequence cursor) throws InterruptedException;

    /**
     * 由生产者调用, 唤醒 waitFor 阻塞在 "该等待策略对象" 上的消费者线程
     */
    void signalAllWhenBlocking();
}
