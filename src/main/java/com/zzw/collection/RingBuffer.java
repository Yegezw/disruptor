package com.zzw.collection;

import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.producer.SingleProducerSequencer;
import com.zzw.relation.wait.WaitStrategy;

/**
 * 环形队列
 */
public class RingBuffer<T> {

    private final T[] elementList;
    private final int ringBufferSize;
    private final int mask;

    // ----------------------------------------

    /**
     * 单线程生产者序号生成器
     */
    private final SingleProducerSequencer singleProducerSequencer;

    // =============================================================================

    public static <E> RingBuffer<E> createSingleProducer(EventFactory<E> factory, int bufferSize, WaitStrategy waitStrategy) {
        SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy);
        return new RingBuffer<>(sequencer, factory);
    }

    public RingBuffer(SingleProducerSequencer singleProducerSequencer, EventFactory<T> eventFactory) {
        int bufferSize = singleProducerSequencer.getRingBufferSize();
        if (Integer.bitCount(bufferSize) != 1) {
            // ringBufferSize 需要是 2 的倍数, 类似 HashMap, 求余数时效率更高
            throw new IllegalArgumentException("BufferSize must be a power of 2");
        }

        this.elementList = (T[]) new Object[bufferSize];
        this.ringBufferSize = bufferSize;
        this.mask = ringBufferSize - 1;
        this.singleProducerSequencer = singleProducerSequencer;

        // 预填充事件对象, 后续生产者和消费者都只会更新事件对象, 不会发生插入、删除等操作, 避免 GC
        for (int i = 0; i < this.elementList.length; i++) {
            this.elementList[i] = eventFactory.newInstance();
        }
    }

    // =============================================================================

    public T get(long sequence) {
        // 由于 ringBuffer 的长度是 2 次幂, mask 为 2 次幂 - 1, 因此可以将求余运算优化为位运算
        int index = (int) (sequence & mask);
        return elementList[index];
    }

    // =============================================================================

    public long next() {
        return this.singleProducerSequencer.next();
    }

    public long next(int n) {
        return this.singleProducerSequencer.next(n);
    }

    public void publish(Long index) {
        this.singleProducerSequencer.publish(index);
    }

    // ----------------------------------------

    public Sequence getCurrentProducerSequence() {
        return this.singleProducerSequencer.getCurrentProducerSequence();
    }

    // ----------------------------------------

    public void addGatingConsumerSequenceList(Sequence consumerSequence) {
        this.singleProducerSequencer.addGatingConsumerSequenceList(consumerSequence);
    }

    public void addGatingConsumerSequenceList(Sequence... consumerSequences) {
        this.singleProducerSequencer.addGatingConsumerSequenceList(consumerSequences);
    }

    // ----------------------------------------

    public SequenceBarrier newBarrier() {
        return this.singleProducerSequencer.newBarrier();
    }

    public SequenceBarrier newBarrier(Sequence... dependenceSequences) {
        return this.singleProducerSequencer.newBarrier(dependenceSequences);
    }
}
