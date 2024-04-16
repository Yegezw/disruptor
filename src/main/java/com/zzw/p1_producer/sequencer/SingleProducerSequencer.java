package com.zzw.p1_producer.sequencer;

import com.zzw.p0_core.Sequence;
import com.zzw.p2_barrier.SequenceBarrier;
import com.zzw.p2_barrier.wait.WaitStrategy;

import java.util.concurrent.locks.LockSupport;

/**
 * 单生产者序号生成器
 */
public class SingleProducerSequencer implements Sequencer
{

    private final int bufferSize;

    // ----------------------------------------

    /**
     * 生产序号, 代表已发布
     */
    private final Sequence     cursor = new Sequence(Sequence.INITIAL_VALUE);
    /**
     * 消费序号, 代表已消费
     */
    private       Sequence     gatingSequence;
    /**
     * 消费者等待策略, 用于唤醒消费者线程
     */
    private final WaitStrategy waitStrategy;

    // ----------------------------------------

    /**
     * 已申请序号
     */
    private long nextValue   = Sequence.INITIAL_VALUE;
    /**
     * 缓存的已消费序号
     */
    private long cachedValue = Sequence.INITIAL_VALUE;

    // =============================================================================

    public SingleProducerSequencer(int bufferSize, WaitStrategy waitStrategy)
    {
        if (bufferSize < 1)
        {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (Integer.bitCount(bufferSize) != 1)
        {
            throw new IllegalArgumentException("bufferSize must be a pow of 2");
        }

        this.bufferSize   = bufferSize;
        this.waitStrategy = waitStrategy;
    }

    @Override
    public int getBufferSize()
    {
        return bufferSize;
    }

    // =============================================================================

    @Override
    public SequenceBarrier newBarrier()
    {
        return new SequenceBarrier(cursor, waitStrategy);
    }

    @Override
    public void addGatingSequence(Sequence gatingSequence)
    {
        this.gatingSequence = gatingSequence;
    }

    // ----------------------------------------

    @Override
    public long next()
    {
        return next(1);
    }

    @Override
    public long next(int n)
    {
        if (n < 1)
        {
            throw new IllegalArgumentException("n must be > 0");
        }

        long nextValue            = this.nextValue;            // 已申请序号
        long nextSequence         = nextValue + n;             // 目标申请序号
        long wrapPoint            = nextSequence - bufferSize; // 上一轮覆盖点
        long cachedGatingSequence = this.cachedValue;          // 缓存的已消费序号

        // wrapPoint <= gatingSequences 才能申请
        if (wrapPoint > cachedGatingSequence)
        {
            // 强一致获取已消费序号
            long sequence;
            while (wrapPoint > (sequence = gatingSequence.get()))
            {
                LockSupport.parkNanos(1L);
            }

            // 缓存已消费序号
            this.cachedValue = sequence;
        }

        // 记录已申请序号
        this.nextValue = nextSequence;
        return nextSequence;
    }

    @Override
    public void publish(long sequence)
    {
        // 保证写操作的有序性
        // Event 的更新 -> Sequence 的更新
        cursor.set(sequence);
        // 唤醒消费者线程
        waitStrategy.signalAllWhenBlocking();
    }
}
