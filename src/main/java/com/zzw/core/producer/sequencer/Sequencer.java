package com.zzw.core.producer.sequencer;

import com.zzw.core.RingBuffer;
import com.zzw.core.Sequence;
import com.zzw.core.consumer.barrier.SequenceBarrier;
import com.zzw.core.producer.EventFactory;

/**
 * 生产者序号生成器
 */
public interface Sequencer
{

    /**
     * @see RingBuffer#RingBuffer(EventFactory, Sequencer) 
     */
    int getBufferSize();

    /**
     * @see RingBuffer#newBarrier()
     */
    SequenceBarrier newBarrier();

    /**
     * @see RingBuffer#addGatingSequence(Sequence)
     */
    void addGatingSequence(Sequence gatingSequence);

    /**
     * @see RingBuffer#next()
     */
    long next();

    /**
     * @see RingBuffer#next(int)
     */
    long next(int n);

    /**
     * @see RingBuffer#publish(long)
     */
    void publish(long sequence);
}
