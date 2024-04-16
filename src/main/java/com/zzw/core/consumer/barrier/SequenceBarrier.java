package com.zzw.core.consumer.barrier;

import com.zzw.core.Sequence;
import com.zzw.core.consumer.BatchEventProcessor;
import com.zzw.core.wait.WaitStrategy;

/**
 * 消费序号屏障
 */
public class SequenceBarrier
{

    /**
     * 生产序号, 代表已发布
     */
    private final Sequence     cursor;
    /**
     * 消费者等待策略, 用于阻塞消费者线程
     */
    private final WaitStrategy waitStrategy;

    public SequenceBarrier(Sequence cursor, WaitStrategy waitStrategy)
    {
        this.cursor       = cursor;
        this.waitStrategy = waitStrategy;
    }

    /**
     * 由消费者调用, 等待给定的序号可供使用
     *
     * @param sequence 申请消费的序号
     * @return 最大可消费序号
     * @see BatchEventProcessor#processEvents()
     */
    public long waitFor(long sequence) throws InterruptedException
    {
        return waitStrategy.waitFor(sequence, cursor);
    }
}
