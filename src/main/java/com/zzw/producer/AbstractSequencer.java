package com.zzw.producer;

import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.WaitStrategy;
import com.zzw.util.SequenceGroups;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class AbstractSequencer implements Sequencer
{

    /**
     * 生产者序号生成器所属 RingBuffer 的大小
     */
    protected final int      bufferSize;
    /**
     * SingleProducerSequencer: 当前已发布的生产序号(区别于 nextValue)<br>
     * MultiProducerSequencer: 多线程生产者共同的已申请序号(可能未发布)
     */
    protected final Sequence cursor = new Sequence();

    // ----------------------------------------

    /**
     * gatingSequences 原子更新器
     */
    private static final AtomicReferenceFieldUpdater<AbstractSequencer, Sequence[]> SEQUENCE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    AbstractSequencer.class,
                    Sequence[].class,
                    "gatingSequences"
            );

    /**
     * 生产者序号生成器所属 RingBuffer 的消费者序号数组
     */
    @SuppressWarnings("all")
    protected volatile Sequence[]   gatingSequences = new Sequence[0];
    /**
     * 消费者等待策略
     */
    protected final    WaitStrategy waitStrategy;

    // =============================================================================

    public AbstractSequencer(int bufferSize, WaitStrategy waitStrategy)
    {
        if (bufferSize < 1)
        {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        if (Integer.bitCount(bufferSize) != 1)
        {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        this.bufferSize   = bufferSize;
        this.waitStrategy = waitStrategy;
    }

    // =============================================================================

    @Override
    public int getBufferSize()
    {
        return bufferSize;
    }

    @Override
    public Sequence getCursor()
    {
        return cursor;
    }

    // ----------------------------------------

    @Override
    public SequenceBarrier newBarrier()
    {
        return new SequenceBarrier(this, waitStrategy, cursor, new Sequence[0]);
    }

    @Override
    public SequenceBarrier newBarrier(Sequence... sequencesToTrack)
    {
        return new SequenceBarrier(this, waitStrategy, cursor, sequencesToTrack);
    }

    @Override
    public void addGatingSequences(Sequence... gatingSequences)
    {
        SequenceGroups.addSequences(this, SEQUENCE_UPDATER, cursor, gatingSequences);
    }

    @Override
    public void removeGatingSequence(Sequence sequence)
    {
        SequenceGroups.removeSequence(this, SEQUENCE_UPDATER, sequence);
    }

    // ----------------------------------------

    @Override
    public String toString()
    {
        return "AbstractSequencer{" +
                "waitStrategy = " + waitStrategy +
                ", cursor = " + cursor +
                ", gatingSequences = " + Arrays.toString(gatingSequences) +
                '}';
    }
}
