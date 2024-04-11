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
    private final int bufferSize;
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
        int bufferSize = singleProducerSequencer.getBufferSize();
        if (Integer.bitCount(bufferSize) != 1) {
            // ringBufferSize 需要是 2 的倍数, 类似 HashMap, 求余数时效率更高
            throw new IllegalArgumentException("BufferSize must be a power of 2");
        }

        this.elementList = (T[]) new Object[bufferSize];
        this.bufferSize = bufferSize;
        this.mask = bufferSize - 1;
        this.singleProducerSequencer = singleProducerSequencer;

        fill(eventFactory);
    }

    /**
     * <p>预填充事件对象
     * <p>后续生产者和消费者都只会更新事件对象, 不会发生插入、删除等操作, 避免 GC
     */
    private void fill(EventFactory<T> eventFactory) {
        for (int i = 0; i < bufferSize; i++) {
            elementList[i] = eventFactory.newInstance();
        }
    }

    // =============================================================================

    public T get(long sequence) {
        // 由于 ringBuffer 的长度是 2 次幂, mask 为 2 次幂 - 1, 因此可以将求余运算优化为位运算
        int index = (int) (sequence & mask);
        return elementList[index];
    }

    // =============================================================================

    public Sequence getCurrentProducerSequence() {
        return singleProducerSequencer.getCurrentProducerSequence();
    }

    // ----------------------------------------

    public SequenceBarrier newBarrier() {
        return singleProducerSequencer.newBarrier();
    }

    public SequenceBarrier newBarrier(Sequence... dependenceSequences) {
        return singleProducerSequencer.newBarrier(dependenceSequences);
    }

    public void addGatingConsumerSequenceList(Sequence consumerSequence) {
        singleProducerSequencer.addGatingConsumerSequenceList(consumerSequence);
    }

    public void addGatingConsumerSequenceList(Sequence... consumerSequences) {
        singleProducerSequencer.addGatingConsumerSequenceList(consumerSequences);
    }

    // ----------------------------------------

    public long next() {
        return singleProducerSequencer.next();
    }

    public long next(int n) {
        return singleProducerSequencer.next(n);
    }

    public void publish(Long index) {
        singleProducerSequencer.publish(index);
    }
}
