package com.zzw.core;

import com.zzw.core.consumer.barrier.SequenceBarrier;
import com.zzw.core.producer.EventFactory;
import com.zzw.core.producer.sequencer.Sequencer;
import com.zzw.core.producer.sequencer.SingleProducerSequencer;
import com.zzw.core.wait.WaitStrategy;

public class RingBuffer<E>
{

    /**
     * 环形数组
     */
    private final Object[] entries;
    private final int      bufferSize;
    private final long     indexMask;

    // ----------------------------------------

    /**
     * 生产者序号生成器
     */
    private final Sequencer sequencer;

    // =============================================================================

    public static <E> RingBuffer<E> createSingleProducer(EventFactory<E> eventFactory,
                                                         int bufferSize,
                                                         WaitStrategy waitStrategy)
    {
        Sequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy);
        return new RingBuffer<>(eventFactory, sequencer);
    }

    public RingBuffer(EventFactory<E> eventFactory, Sequencer sequencer)
    {
        this.sequencer  = sequencer;
        this.bufferSize = sequencer.getBufferSize();
        if (bufferSize < 1)
        {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (Integer.bitCount(bufferSize) != 1)
        {
            throw new IllegalArgumentException("bufferSize must be a pow of 2");
        }

        this.indexMask = bufferSize - 1;
        this.entries   = new Object[bufferSize];
        fill(eventFactory);
    }

    private void fill(EventFactory<E> eventFactory)
    {
        for (int i = 0; i < bufferSize; i++)
        {
            entries[i] = eventFactory.newInstance();
        }
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    @SuppressWarnings("unchecked")
    public E get(long sequence)
    {
        int index = (int) (sequence & indexMask);
        return (E) entries[index];
    }

    // =============================================================================

    /**
     * 创建消费序号屏障
     */
    public SequenceBarrier newBarrier()
    {
        return sequencer.newBarrier();
    }

    /**
     * 向 RingBuffer 注册需要监控的消费序号, 用于控制生产速度
     */
    public void addGatingSequence(Sequence gatingSequence)
    {
        sequencer.addGatingSequence(gatingSequence);
    }

    /**
     * 申请 1 个生产序号
     */
    public long next()
    {
        return sequencer.next();
    }

    /**
     * 申请 n 个生产序号
     */
    public long next(int n)
    {
        return sequencer.next(n);
    }

    /**
     * 向 RingBuffer 发布 sequence 号事件
     */
    public void publish(long sequence)
    {
        sequencer.publish(sequence);
    }
}
