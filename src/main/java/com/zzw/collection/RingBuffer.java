package com.zzw.collection;

import com.zzw.producer.MultiProducerSequencer;
import com.zzw.producer.Sequencer;
import com.zzw.producer.SingleProducerSequencer;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.WaitStrategy;

/**
 * 环形队列
 */
public class RingBuffer<E> {

    private final E[] elementList;
    private final int bufferSize;
    private final int mask;

    // ----------------------------------------

    /**
     * "单线程 OR 多线程" 生产者序号生成器
     */
    private final Sequencer producerSequencer;

    // =============================================================================

    public static <E> RingBuffer<E> createSingleProducer(EventFactory<E> factory, int bufferSize, WaitStrategy waitStrategy) {
        SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy);
        return new RingBuffer<>(sequencer, factory);
    }

    public static <E> RingBuffer<E> createMultiProducer(EventFactory<E> factory, int bufferSize, WaitStrategy waitStrategy) {
        MultiProducerSequencer sequencer = new MultiProducerSequencer(bufferSize, waitStrategy);
        return new RingBuffer<>(sequencer, factory);
    }

    public RingBuffer(Sequencer producerSequencer, EventFactory<E> eventFactory) {
        int bufferSize = producerSequencer.getBufferSize();
        if (Integer.bitCount(bufferSize) != 1) {
            // ringBufferSize 需要是 2 的倍数, 类似 HashMap, 求余数时效率更高
            throw new IllegalArgumentException("BufferSize must be a power of 2");
        }

        this.elementList = (E[]) new Object[bufferSize];
        this.bufferSize = bufferSize;
        this.mask = bufferSize - 1;
        this.producerSequencer = producerSequencer;

        fill(eventFactory);
    }

    /**
     * <p>预填充事件对象
     * <p>后续生产者和消费者都只会更新事件对象, 不会发生插入、删除等操作, 避免 GC
     */
    private void fill(EventFactory<E> eventFactory) {
        for (int i = 0; i < bufferSize; i++) {
            elementList[i] = eventFactory.newInstance();
        }
    }

    // =============================================================================

    public E get(long sequence) {
        // 由于 ringBuffer 的长度是 2 次幂, mask 为 2 次幂 - 1, 因此可以将求余运算优化为位运算
        int index = (int) (sequence & mask);
        return elementList[index];
    }

    // =============================================================================

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * @see Sequencer#getCurrentProducerSequence()
     */
    public Sequence getCurrentProducerSequence() {
        return producerSequencer.getCurrentProducerSequence();
    }

    // ----------------------------------------

    /**
     * @see Sequencer#newBarrier()
     */
    public SequenceBarrier newBarrier() {
        return producerSequencer.newBarrier();
    }

    /**
     * @see Sequencer#newBarrier(Sequence...)
     */
    public SequenceBarrier newBarrier(Sequence... dependenceSequences) {
        return producerSequencer.newBarrier(dependenceSequences);
    }

    /**
     * @see Sequencer#addGatingConsumerSequence(Sequence)
     */
    public void addGatingConsumerSequence(Sequence consumerSequence) {
        producerSequencer.addGatingConsumerSequence(consumerSequence);
    }

    /**
     * @see Sequencer#addGatingConsumerSequenceList(Sequence...)
     */
    public void addGatingConsumerSequenceList(Sequence... consumerSequences) {
        producerSequencer.addGatingConsumerSequenceList(consumerSequences);
    }

    /**
     * @see Sequencer#removeGatingConsumerSequence(Sequence)
     */
    public void removeGatingConsumerSequence(Sequence sequenceNeedRemove) {
        producerSequencer.removeGatingConsumerSequence(sequenceNeedRemove);
    }

    // ----------------------------------------

    /**
     * @see Sequencer#next()
     */
    public long next() {
        return producerSequencer.next();
    }

    /**
     * @see Sequencer#next(int)
     */
    public long next(int n) {
        return producerSequencer.next(n);
    }

    /**
     * @see Sequencer#publish(long)
     */
    public void publish(Long index) {
        producerSequencer.publish(index);
    }
}
