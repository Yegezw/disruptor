package com.zzw.core.producer.sequencer;

import com.zzw.core.Sequence;
import com.zzw.core.consumer.barrier.SequenceBarrier;

/**
 * 生产者序号生成器
 */
public interface Sequencer
{

    int getBufferSize();

    SequenceBarrier newBarrier();

    void addGatingSequence(Sequence gatingSequence);

    long next();

    long next(int n);

    void publish(long sequence);
}
